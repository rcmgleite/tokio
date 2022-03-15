use bytes::Bytes;
use futures::stream::FuturesUnordered;
use futures::FutureExt;
use futures::StreamExt;
use std::collections::BTreeMap;
use std::sync::Arc;
use tokio::io::AsyncReadExt;
use tokio::net::TcpStream;
use tokio::select;
use tokio::sync::mpsc::{channel, Receiver, Sender};
use tokio::sync::Mutex as AsyncMutex;
use tokio::time::Instant;

/// Smaller wraper around a `Source` that reads from the inner `Source`
/// using a separate tokio::task and a channel
#[allow(dead_code)]
struct BufferedSource {
    id: usize,
    is_done: bool,
    inner: Arc<AsyncMutex<Source>>,
    data_channel: Option<Receiver<Option<Bytes>>>,
}

#[allow(dead_code)]
impl BufferedSource {
    fn new(inner: Source) -> Self {
        Self {
            id: inner.id,
            is_done: false,
            inner: Arc::new(AsyncMutex::new(inner)),
            data_channel: None,
        }
    }

    fn is_done(&self) -> bool {
        self.is_done
    }

    async fn connect(&mut self) {
        {
            self.inner.lock().await.connect().await;
        }

        let (data_tx, data_rx) = channel(1);
        tokio::spawn(read_task(self.inner.clone(), data_tx));

        self.data_channel = Some(data_rx);
    }

    async fn read(&mut self) -> Option<Bytes> {
        if let Some(data_channel) = &mut self.data_channel {
            if let Some(data) = data_channel.recv().await {
                return data;
            } else {
                self.is_done = true;
            }
        } else {
            self.is_done = true;
        }

        None
    }
}

async fn read_task(inner_source: Arc<AsyncMutex<Source>>, data_channel: Sender<Option<Bytes>>) {
    loop {
        let mut locked_source = inner_source.lock().await;
        match locked_source.read().await {
            Some(data) => {
                // let's drop the locked_source before sending data to the send channel
                // otherwise we might deadlock since the channel might be full
                drop(locked_source);

                if data_channel.send(Some(data)).await.is_err() {
                    // if we fail to send data, it means that the other half of the channel was closed.
                    // so we need to stop the reader task to avoid leaking it.
                    break;
                }
            }
            None => {
                let _ = data_channel.send(None).await;
                break;
            }
        }
    }
}

/// A Source is just a wrapper around a TcpStream.
///  it provides:
///  - `connect` method - that establishes a TCP connection with a specific source host
///  - `read` method - returns `Bytes` read from the underlying TCP socket in chunks of 4096 kb
#[derive(Debug)]
struct Source {
    id: usize,
    stream: Option<TcpStream>,
    bytes_read: usize,
    size: usize,
}

impl Source {
    fn new(id: usize, size: usize) -> Self {
        Self {
            id,
            stream: None,
            bytes_read: 0,
            size,
        }
    }

    async fn connect(&mut self) {
        let addr = format!("127.0.0.1:{}", 4000 + self.id);
        let stream = TcpStream::connect(&addr).await.unwrap();
        self.stream = Some(stream);
    }

    async fn read(&mut self) -> Option<Bytes> {
        if let Some(stream) = &mut self.stream {
            let bytes_to_read = std::cmp::min(4096, self.size - self.bytes_read);
            if bytes_to_read > 0 {
                // let start = Instant::now();
                let mut buf = Vec::with_capacity(bytes_to_read);
                unsafe {
                    buf.set_len(bytes_to_read);
                }

                if let Err(err) = stream.read_exact(&mut buf[..]).await {
                    println!("{:?}", err);
                    println!(
                        "[DEBUG] - bytes_to_read: {}, size: {}, bytes_read: {}",
                        bytes_to_read, self.size, self.bytes_read
                    );
                }

                self.bytes_read += bytes_to_read;

                // println!("read from source: {} took: {} micros", self.id, start.elapsed().as_micros());
                Some(Bytes::from(buf))
            } else {
                None
            }
        } else {
            panic!("ouch");
        }
    }

    #[allow(dead_code)]
    fn is_done(&self) -> bool {
        self.bytes_read == self.size
    }
}

type SourceUsed = BufferedSource;
// type SourceUsed = Source;

async fn read_from_source(mut source: Box<SourceUsed>) -> (Box<SourceUsed>, Option<Bytes>) {
    let maybe_bytes = source.read().await;
    (source, maybe_bytes)
}

/// A single request will:
///  1. read from all required 20 sources
///  2. aggregate the read bytes in a BTreeMap
///  3. re-schedule sources to be read as soon as all 20 are done reading
async fn do_one_request(id: usize) -> (usize, usize) {
    let mut read_futures = FuturesUnordered::new();

    let mut sources = (0..20)
        .map(|id| {
            Box::new(BufferedSource::new(Source::new(id, 1_000_002)))
            // Box::new(Source::new(id, 1_000_002))
        })
        .collect::<Vec<Box<SourceUsed>>>();

    loop {
        let source = sources.pop();
        if source.is_none() {
            break;
        }

        let mut source = source.unwrap();
        source.connect().await;
        read_futures.push(read_from_source(source));
    }

    let mut chunks = BTreeMap::new();
    let mut total_bytes_read = 0;
    loop {
        select! {
            Some((source, maybe_bytes)) = read_futures.next() => {
                if let Some(bytes) = maybe_bytes {
                    total_bytes_read += bytes.len();
                    chunks.insert(source.id, bytes);
                }

                if !source.is_done() {
                    sources.push(source);
                }
            },
            else => {
                if read_futures.len() == 0 && sources.len() != 0 {
                    loop {
                        let source = sources.pop();
                        if source.is_none() {
                            break;
                        }

                        let source = source.unwrap();
                        read_futures.push(read_from_source(source));
                    }
                } else {
                    break;
                }
            }
        }
    }

    (id, total_bytes_read)
}

#[tokio::main]
async fn main() {
    tokio::spawn(async move {
        let start = Instant::now();
        let n_concurrent_requests = 1;

        let mut total_bytes_read = 0;
        let mut futures = FuturesUnordered::new();
        for i in 0..n_concurrent_requests {
            futures.push(do_one_request(i))
        }
        let duration_future = tokio::time::sleep(tokio::time::Duration::from_secs(60)).fuse();
        tokio::pin!(duration_future);

        let mut request_counter = 0;
        loop {
            select! {
                Some((id, bytes_read)) = futures.next() => {
                    request_counter += 1;
                    total_bytes_read += bytes_read;
                    futures.push(do_one_request(id));
                }
                _ = &mut duration_future => break,
            }
        }

        let elapsed = start.elapsed();

        println!(
            "Done!\n\trequests: {}\n\telapsed: {:?}\n\tThroughput: {} Mb/s",
            request_counter,
            elapsed,
            total_bytes_read / elapsed.as_secs() as usize / 1024 / 1024
        );
    })
    .await
    .unwrap();
}
