//! TCP over io_uring, driven from tokio.
//!
//! A [`Driver`] owns one ring. Sockets queue operations on it, and a driver task submits them in
//! batches (one `io_uring_enter` for everything queued during a scheduler tick) and wakes the
//! sockets as their completions arrive. tokio learns about completions by polling the ring's
//! file descriptor, which turns readable whenever the completion queue is not empty.
//!
//! The kernel reads from and writes to an operation's buffers until it completes, even if the
//! socket that started it is gone. Buffers and addresses therefore move into the driver when an
//! operation starts and only come back with its completion; dropping an unfinished operation
//! cancels it, and the driver frees its buffers once the kernel is done with them.

use std::future::{Future, poll_fn};
use std::io::{self, IoSlice};
use std::mem;
use std::net::{Shutdown, SocketAddr};
use std::os::fd::{AsRawFd, RawFd};
use std::pin::Pin;
use std::sync::{Arc, Mutex, MutexGuard, PoisonError, Weak};
use std::task::{Context, Poll, Waker, ready};

use io_uring::{IoUring, Probe, opcode, squeue, types};
use slab::Slab;
use socket2::{Domain, SockAddr, Socket, Type};
use tokio::io::unix::AsyncFd;
use tokio::io::{AsyncRead, AsyncWrite, Interest, ReadBuf};

/// Submission queue entries; the completion queue is twice as large.
const ENTRIES: u32 = 1024;
/// Bytes received at once per socket.
const READ_BUFFER: usize = 16 * 1024;
/// Most bytes accepted by a single write.
const MAX_WRITE: usize = 64 * 1024;
/// `user_data` of cancellations, whose completions are ignored.
const CANCEL: u64 = u64::MAX;

/// The slot of an operation: the low half of its `user_data`, the high half is a generation.
fn slot(user_data: u64) -> usize {
    (user_data & u64::from(u32::MAX)) as usize
}

/// What an operation needs to stay alive until it completes.
enum Resources {
    Buffer(Vec<u8>),
    Address(#[allow(dead_code)] Box<SockAddr>),
}

impl Resources {
    fn into_buffer(self) -> Vec<u8> {
        match self {
            Resources::Buffer(buf) => buf,
            Resources::Address(_) => unreachable!("the operation was started with a buffer"),
        }
    }
}

enum State {
    Waiting(Option<Waker>),
    Done(i32),
    /// Its [`Op`] was dropped; the result is thrown away.
    Abandoned,
}

struct Slot {
    /// Tells completions of an earlier operation in the same slot apart.
    user_data: u64,
    state: State,
    resources: Resources,
}

struct Inner {
    ring: IoUring,
    ops: Slab<Slot>,
    generation: u32,
    /// Entries were queued but not yet submitted.
    unsubmitted: bool,
    /// Wakes the driver task to submit queued entries.
    driver: Option<Waker>,
}

impl Inner {
    fn push(&mut self, entry: &squeue::Entry) {
        // SAFETY: the caller keeps everything the entry points to alive until it completes.
        while unsafe { self.ring.submission().push(entry) }.is_err() {
            self.submit();
        }
        self.unsubmitted = true;
        if let Some(driver) = self.driver.take() {
            driver.wake();
        }
    }

    fn submit(&mut self) {
        loop {
            match self.ring.submit() {
                Ok(_) => break,
                // The completion queue overflowed: make room, then try again.
                Err(e) if e.raw_os_error() == Some(libc::EBUSY) => self.complete(),
                Err(e) if matches!(e.raw_os_error(), Some(libc::EAGAIN | libc::EINTR)) => {}
                Err(e) => panic!("gumo: cannot submit to io_uring: {e}"),
            }
        }
        // The kernel may take only part of the queue, for lack of memory.
        self.unsubmitted = !self.ring.submission().is_empty();
    }

    /// Hands every available completion to its operation.
    fn complete(&mut self) {
        for cqe in self.ring.completion() {
            let user_data = cqe.user_data();
            if user_data == CANCEL {
                continue;
            }
            let id = slot(user_data);
            let Some(slot) = self.ops.get_mut(id).filter(|s| s.user_data == user_data) else {
                continue;
            };
            match mem::replace(&mut slot.state, State::Done(cqe.result())) {
                State::Waiting(waker) => waker.into_iter().for_each(Waker::wake),
                State::Abandoned => drop(self.ops.remove(id)),
                State::Done(_) => unreachable!("an operation completed twice"),
            }
        }
    }

    fn cancel(&mut self, user_data: u64) {
        let entry = opcode::AsyncCancel::new(user_data)
            .build()
            .user_data(CANCEL);
        self.push(&entry);
    }
}

/// The io_uring file descriptor, which tokio watches. Owned by the [`IoUring`].
struct RingFd(RawFd);

impl AsRawFd for RingFd {
    fn as_raw_fd(&self) -> RawFd {
        self.0
    }
}

pub(crate) struct Driver {
    // Declared first so that it deregisters before the ring closes the descriptor.
    fd: AsyncFd<RingFd>,
    inner: Mutex<Inner>,
}

impl Driver {
    /// Sets up a ring and spawns its driver task on the current runtime. Fails if the kernel
    /// lacks io_uring, or the operations used here, or forbids it (as container sandboxes do).
    pub(crate) fn new() -> io::Result<Arc<Self>> {
        let ring = IoUring::builder()
            .setup_cqsize(2 * ENTRIES)
            .build(ENTRIES)?;
        let mut probe = Probe::new();
        ring.submitter().register_probe(&mut probe)?;
        let codes = [
            opcode::Connect::CODE,
            opcode::Send::CODE,
            opcode::Recv::CODE,
            opcode::AsyncCancel::CODE,
        ];
        if !codes.iter().all(|&code| probe.is_supported(code)) {
            return Err(io::Error::new(
                io::ErrorKind::Unsupported,
                "io_uring lacks network operations",
            ));
        }
        // SAFETY: the ring keeps the descriptor open, and `Driver` drops `fd` before the ring.
        let fd = unsafe {
            AsyncFd::register_with_interest(RingFd(ring.as_raw_fd()), Interest::READABLE)
        }?;
        let driver = Arc::new(Driver {
            fd,
            inner: Mutex::new(Inner {
                ring,
                ops: Slab::new(),
                generation: 0,
                unsubmitted: false,
                driver: None,
            }),
        });
        tokio::spawn(drive(Arc::downgrade(&driver)));
        Ok(driver)
    }

    fn lock(&self) -> MutexGuard<'_, Inner> {
        self.inner.lock().unwrap_or_else(PoisonError::into_inner)
    }

    /// Starts an operation, keeping `resources` alive until it completes.
    ///
    /// # Safety
    ///
    /// Every pointer in `entry` must point into `resources`' heap allocations, or stay valid
    /// for as long as the driver lives.
    unsafe fn start(self: &Arc<Self>, entry: squeue::Entry, resources: Resources) -> Op {
        let mut inner = self.lock();
        inner.generation = inner.generation.wrapping_add(1);
        let generation = u64::from(inner.generation) << 32;
        let slot = inner.ops.vacant_entry();
        let user_data = generation | slot.key() as u64;
        slot.insert(Slot {
            user_data,
            state: State::Waiting(None),
            resources,
        });
        inner.push(&entry.user_data(user_data));
        Op {
            driver: self.clone(),
            user_data: Some(user_data),
        }
    }

    /// Submits what was queued and hands out completions, until there is nothing left to do.
    fn poll_drive(&self, cx: &mut Context<'_>) -> Poll<()> {
        loop {
            {
                let mut inner = self.lock();
                inner.driver = Some(cx.waker().clone());
                if inner.unsubmitted || inner.ring.submission().cq_overflow() {
                    inner.submit();
                }
                inner.complete();
                if inner.unsubmitted {
                    // Try the rest again soon, without waiting for a completion.
                    cx.waker().wake_by_ref();
                }
            }
            match self.fd.poll_read_ready(cx) {
                // Clear before draining (on the next iteration), so that a completion that
                // arrives in between makes the descriptor ready again.
                Poll::Ready(Ok(mut guard)) => guard.clear_ready(),
                Poll::Ready(Err(e)) => panic!("gumo: cannot poll io_uring: {e}"),
                Poll::Pending => return Poll::Pending,
            }
        }
    }
}

impl Drop for Driver {
    fn drop(&mut self) {
        let inner = self.inner.get_mut().unwrap_or_else(PoisonError::into_inner);
        // The kernel must be done with every buffer before the buffers are freed.
        // Live operations hold the driver, so only abandoned ones are left; their completions
        // may still be outstanding.
        inner
            .ops
            .retain(|_, slot| !matches!(slot.state, State::Done(_)));
        let pending: Vec<u64> = inner.ops.iter().map(|(_, slot)| slot.user_data).collect();
        for user_data in pending {
            inner.cancel(user_data);
        }
        for (_, slot) in &mut inner.ops {
            slot.state = State::Abandoned;
        }
        while !inner.ops.is_empty() {
            match inner.ring.submit_and_wait(1) {
                Ok(_) => inner.complete(),
                Err(e) if e.raw_os_error() == Some(libc::EINTR) => {}
                Err(e) if e.raw_os_error() == Some(libc::EBUSY) => inner.complete(),
                Err(e) => panic!("gumo: cannot wait for io_uring: {e}"),
            }
        }
        if let Some(driver) = inner.driver.take() {
            // Lets the driver task notice that the driver is gone.
            driver.wake();
        }
    }
}

/// Holds a weak reference so that the driver goes away with its last socket.
async fn drive(driver: Weak<Driver>) {
    poll_fn(|cx| match driver.upgrade() {
        Some(driver) => driver.poll_drive(cx),
        None => Poll::Ready(()),
    })
    .await;
}

/// An operation in flight; resolves to the kernel's result and the operation's resources.
struct Op {
    driver: Arc<Driver>,
    /// `None` once completed.
    user_data: Option<u64>,
}

impl Future for Op {
    type Output = (i32, Resources);

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let user_data = self
            .user_data
            .expect("gumo: io_uring operation polled after completion");
        let id = slot(user_data);
        let mut inner = self.driver.lock();
        match &mut inner.ops[id].state {
            State::Done(result) => {
                let result = *result;
                let slot = inner.ops.remove(id);
                drop(inner);
                self.user_data = None;
                Poll::Ready((result, slot.resources))
            }
            State::Waiting(waker) => {
                if !waker.as_ref().is_some_and(|w| w.will_wake(cx.waker())) {
                    *waker = Some(cx.waker().clone());
                }
                Poll::Pending
            }
            State::Abandoned => unreachable!("a live operation was abandoned"),
        }
    }
}

impl Drop for Op {
    fn drop(&mut self) {
        let Some(user_data) = self.user_data else {
            return;
        };
        let id = slot(user_data);
        let mut inner = self.driver.lock();
        if let State::Done(_) = inner.ops[id].state {
            inner.ops.remove(id);
        } else {
            inner.ops[id].state = State::Abandoned;
            inner.cancel(user_data);
        }
    }
}

/// Retries on these instead of failing.
fn is_transient(result: i32) -> bool {
    matches!(-result, libc::EAGAIN | libc::EINTR)
}

enum Read {
    /// `buf[pos..len]` was received but not yet read.
    Idle {
        buf: Vec<u8>,
        pos: usize,
        len: usize,
    },
    Busy(Op),
}

enum Write {
    Idle(Vec<u8>),
    /// Sending the whole buffer; `sent` bytes of it are already gone.
    Busy {
        op: Op,
        sent: usize,
    },
}

/// A TCP connection whose reads and writes go through io_uring.
pub(crate) struct TcpStream {
    socket: std::net::TcpStream,
    driver: Arc<Driver>,
    read: Read,
    write: Write,
}

impl TcpStream {
    /// Connects to the first address of `host` that accepts.
    pub(crate) async fn connect(driver: &Arc<Driver>, host: &str, port: u16) -> io::Result<Self> {
        let mut last_error = None;
        for address in tokio::net::lookup_host((host, port)).await? {
            match Self::connect_to(driver, address).await {
                Ok(stream) => return Ok(stream),
                Err(e) => last_error = Some(e),
            }
        }
        Err(last_error.unwrap_or_else(|| {
            io::Error::new(io::ErrorKind::InvalidInput, "no addresses to connect to")
        }))
    }

    async fn connect_to(driver: &Arc<Driver>, address: SocketAddr) -> io::Result<Self> {
        // Blocking on purpose: io_uring waits for readiness itself, whereas on a non-blocking
        // socket it would fail with EAGAIN instead.
        let socket = Socket::new(Domain::for_address(address), Type::STREAM, None)?;
        let address = Box::new(SockAddr::from(address));
        let entry = opcode::Connect::new(
            types::Fd(socket.as_raw_fd()),
            address.as_ptr().cast(),
            address.len(),
        )
        .build();
        // SAFETY: the address lives on the heap, inside the resources.
        let (result, _) = unsafe { driver.start(entry, Resources::Address(address)) }.await;
        if result < 0 {
            return Err(io::Error::from_raw_os_error(-result));
        }
        socket.set_tcp_nodelay(true)?;
        Ok(TcpStream {
            socket: socket.into(),
            driver: driver.clone(),
            read: Read::Idle {
                buf: vec![0; READ_BUFFER],
                pos: 0,
                len: 0,
            },
            write: Write::Idle(Vec::with_capacity(MAX_WRITE)),
        })
    }

    fn fd(&self) -> types::Fd {
        types::Fd(self.socket.as_raw_fd())
    }

    /// Sends `buf[sent..]`.
    fn send(&mut self, buf: Vec<u8>, sent: usize) {
        let rest = &buf[sent..];
        let entry = opcode::Send::new(self.fd(), rest.as_ptr(), rest.len() as u32)
            .flags(libc::MSG_NOSIGNAL)
            .build();
        // SAFETY: the buffer's heap allocation moves into the resources unchanged.
        let op = unsafe { self.driver.start(entry, Resources::Buffer(buf)) };
        self.write = Write::Busy { op, sent };
    }

    /// Waits until the kernel took everything written so far.
    fn poll_sent(&mut self, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        loop {
            let Write::Busy { op, sent } = &mut self.write else {
                return Poll::Ready(Ok(()));
            };
            let (result, resources) = ready!(Pin::new(op).poll(cx));
            let sent = *sent;
            let buf = resources.into_buffer();
            if is_transient(result) {
                self.send(buf, sent);
                continue;
            }
            self.write = Write::Idle(Vec::new());
            let sent = match result {
                ..0 => return Poll::Ready(Err(io::Error::from_raw_os_error(-result))),
                0 => return Poll::Ready(Err(io::ErrorKind::WriteZero.into())),
                n => sent + n as usize,
            };
            if sent < buf.len() {
                self.send(buf, sent);
            } else {
                self.write = Write::Idle(buf);
            }
        }
    }
}

impl AsyncRead for TcpStream {
    fn poll_read(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        out: &mut ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        let this = self.get_mut();
        loop {
            match &mut this.read {
                Read::Idle { buf, pos, len } => {
                    if *pos < *len || out.remaining() == 0 {
                        let n = out.remaining().min(*len - *pos);
                        out.put_slice(&buf[*pos..*pos + n]);
                        *pos += n;
                        return Poll::Ready(Ok(()));
                    }
                    let mut buf = mem::take(buf);
                    let entry =
                        opcode::Recv::new(this.fd(), buf.as_mut_ptr(), buf.len() as u32).build();
                    // SAFETY: the buffer's heap allocation moves into the resources unchanged.
                    this.read =
                        Read::Busy(unsafe { this.driver.start(entry, Resources::Buffer(buf)) });
                }
                Read::Busy(op) => {
                    let (result, resources) = ready!(Pin::new(op).poll(cx));
                    let buf = resources.into_buffer();
                    let len = result.max(0) as usize;
                    this.read = Read::Idle { buf, pos: 0, len };
                    match result {
                        // End of stream: nothing was read.
                        0 => return Poll::Ready(Ok(())),
                        _ if is_transient(result) => {}
                        ..0 => return Poll::Ready(Err(io::Error::from_raw_os_error(-result))),
                        _ => {}
                    }
                }
            }
        }
    }
}

impl AsyncWrite for TcpStream {
    fn poll_write(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        data: &[u8],
    ) -> Poll<io::Result<usize>> {
        self.poll_write_vectored(cx, &[IoSlice::new(data)])
    }

    /// Copies the data and starts sending it. A failed send is reported by the next write,
    /// flush or shutdown.
    fn poll_write_vectored(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        data: &[IoSlice<'_>],
    ) -> Poll<io::Result<usize>> {
        let this = self.get_mut();
        ready!(this.poll_sent(cx))?;
        let Write::Idle(buf) = &mut this.write else {
            unreachable!("nothing is in flight once sent");
        };
        let mut buf = mem::take(buf);
        buf.clear();
        for slice in data {
            let room = MAX_WRITE - buf.len();
            buf.extend_from_slice(&slice[..slice.len().min(room)]);
            if buf.len() == MAX_WRITE {
                break;
            }
        }
        let n = buf.len();
        if n == 0 {
            this.write = Write::Idle(buf);
        } else {
            this.send(buf, 0);
        }
        Poll::Ready(Ok(n))
    }

    fn is_write_vectored(&self) -> bool {
        true
    }

    fn poll_flush(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        self.get_mut().poll_sent(cx)
    }

    fn poll_shutdown(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        let this = self.get_mut();
        ready!(this.poll_sent(cx))?;
        Poll::Ready(this.socket.shutdown(Shutdown::Write))
    }
}
