# paging-stream
A utility to make it trivial to expose paginated resources as `futures::Stream`.

Use this crate to hide repository implementation details from client code. Client code may instead request the full collection at once and then the stream will lazily paginate over it, yielding results in order as they become available.

## Getting started
```toml
[dependencies]
paging-stream = "0.1.0"
```

## Example
```rs
use futures::{Stream, StreamExt};
use paging_stream::{Paginated, PagingStream};

impl Paginated for YourRepository {
    type Params = SomeParams;
    type Item = usize;
    type Error = ();

    fn fetch_page(
        &self,
        params: Self::Params,
    ) -> impl Future<Output = Result<(Vec<Self::Item>, Option<Self::Params>), Self::Error>>
    + Send
    + 'static {
        async move {
            // Your async request here
        }
    }
}

#[tokio::main]
async fn main() {
    let repository = SomeRepository { /* whatever fields */};

    let params = SomeParams { /* pagination params */};

    let mut stream = PagingStream::new(repository, params);

    while let Some(value) = stream.next().await {
        // Consume the stream and do something with the values
    }
}
```

[See the tests for a more implementation examples](src/lib.rs)
