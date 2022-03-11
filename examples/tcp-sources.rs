use std::path::PathBuf;
// use rand::{thread_rng, Rng};
use bytes::Bytes;
use std::str::FromStr;
use tokio::fs::File;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpListener;

#[tokio::main]
async fn main() {
    let content_path = PathBuf::from_str("./examples/data_1Mb").unwrap();
    let n_sources = 20;

    let mut counter = 0;
    let mut handles = Vec::new();

    let mut file = File::open(content_path.clone()).await.unwrap();
    let content_len = file.metadata().await.unwrap().len() as usize;
    let mut content = Vec::with_capacity(content_len as usize);
    file.read_to_end(&mut content).await.unwrap();
    let content = Bytes::from(content);

    loop {
        if counter == n_sources {
            break;
        }

        let handle = tokio::spawn(start_source(
            format!("127.0.0.1:{}", 4000 + counter),
            content.clone(),
        ));
        handles.push(handle);
        counter += 1;
    }

    for handle in handles {
        handle.await.unwrap();
    }
}

async fn start_source(addr: String, content: Bytes) {
    let listener = TcpListener::bind(&addr).await.unwrap();

    loop {
        let (mut socket, _) = listener.accept().await.unwrap();
        let content = content.clone();
        tokio::spawn(async move {
            let mut bytes_sent = 0;
            let chunk_size = 4096;
            loop {
                let bytes_to_send = std::cmp::min(content.len() - bytes_sent, chunk_size);

                if bytes_to_send == 0 {
                    break;
                }

                // if thread_rng().gen_range(0.0..1.0) < 0.5 {
                //     tokio::time::sleep(tokio::time::Duration::from_millis(10)).await;
                // }

                if let Err(_) = socket
                    .write_all(&content[bytes_sent..bytes_sent + bytes_to_send])
                    .await
                {
                    // println!("{:?}", err);
                    break;
                }

                bytes_sent += bytes_to_send;
            }
        });
    }
}
