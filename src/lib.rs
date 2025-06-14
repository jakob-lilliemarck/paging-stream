//! A utility to simplify consuming paginated data sources as a `futures::Stream`.
//!
//! This crate provides the `Paginated` trait, which you implement for your API client
//! or repository, and the `PagingStream` struct, which wraps your type and yields
//! items as a stream. This allows consumers of your API to work with a continuous
//! stream of data, abstracting away the underlying pagination logic.
//!
//! ## Example
//!
//! ```rust
//! use futures::{StreamExt, future::Future};
//! use paging_stream::{Paginated, PagingStream};
//!
//! // 1. Define a client/repository struct.
//! struct MyApiClient;
//!
//! // 2. Define your types for parameters, items, and errors.
//! struct MyParams {
//!     since: usize,
//!     until: usize,
//!     limit: usize
//! }
//!
//! // 3. Implement the `Paginated` trait for your client.
//! impl Paginated for MyApiClient {
//!     type Params = MyParams;
//!     type Item = usize;
//!     type Error = ();
//!
//!     fn fetch_page(
//!         &self,
//!         params: Self::Params,
//!     ) -> impl Future<Output = Result<(Vec<Self::Item>, Option<Self::Params>), Self::Error>>
//!     + Send
//!     + 'static {
//!         async move {
//!             // Replace with your actual asynchronous data fetching logic.
//!             //
//!             // - `params`: Contains the necessary information to fetch the current page.
//!             // - Return `Ok((items, next_params))` where:
//!             //   - `items`: A `Vec` of fetched items for the current page.
//!             //   - `next_params`: An `Option<Self::Params>`:
//!             //     - `Some(params)`: Contains the parameters needed to fetch the *next* page.
//!             //     - `None`: Signifies that there are no more pages.
//!             // - Return `Err(your_error)` if fetching fails.
//!            Ok((Vec::new(), None)) // Placeholder for example
//!         }
//!     }
//! }
//!
//! async fn consume_as_stream() {
//!     let client = MyApiClient;
//!     let initial_params = MyParams {
//!         since: 0,
//!         until: 100,
//!         limit: 20
//!     };
//!
//!     // 4. Create a `PagingStream`.
//!     let mut stream = PagingStream::new(client, initial_params);
//!
//!     // 5. Consume the stream.
//!     while let Some(result) = stream.next().await {
//!         match result {
//!             Ok(item) => { /* process `item` */ }
//!             Err(e) => { /* handle `e` */ break; }
//!         }
//!     }
//! }
//! ```

use futures::Stream;
use std::collections::VecDeque;
use std::pin::Pin;
use std::task::Poll;

/// Represents a data source that can be paginated.
pub trait Paginated {
    /// The type of parameters used to request a page (e.g., page number, cursor, offset).
    type Params: Unpin;
    /// The type of item that the stream will yield.
    type Item: Unpin;
    /// The type of error that can occur during page fetching.
    type Error;

    /// Asynchronously fetches a single page of items.
    ///
    /// This method takes the current `params` and should return a `Result` containing:
    /// - `Ok((Vec<Self::Item>, Option<Self::Params>))`:
    ///   - A `Vec` of items for the current page.
    ///   - An `Option` for the next page's parameters. `Some(next_params)` indicates
    ///     there might be more data, and `None` signifies the end of the data source.
    /// - `Err(Self::Error)`: If an error occurs during fetching.
    ///
    /// The returned `Future` must be `Send + 'static`.
    fn fetch_page(
        &self,
        params: Self::Params,
    ) -> impl Future<Output = Result<(Vec<Self::Item>, Option<Self::Params>), Self::Error>>
    + Send
    + 'static;
}

type MaybeInFlight<T, U, E> =
    Option<Pin<Box<dyn Future<Output = Result<(Vec<T>, Option<U>), E>> + Send + 'static>>>;

/// A stream that wraps a `Paginated` type to provide continuous, lazy-loaded data.
///
/// `PagingStream` handles the logic of fetching pages, buffering items, and
/// managing the state of requests. It polls the `Paginated::fetch_page` method
/// as needed when the stream is consumed.
///
/// # Type Parameters
/// - `T`: The type that implements the `Paginated` trait. It must also be `Unpin`.
pub struct PagingStream<T>
where
    T: Paginated,
    T: Unpin,
{
    client: T,
    params: Option<T::Params>,
    buffer: VecDeque<T::Item>,
    request: MaybeInFlight<T::Item, T::Params, T::Error>,
}

impl<T> PagingStream<T>
where
    T: Paginated,
    T: Unpin,
{
    /// Creates a new `PagingStream`.
    ///
    /// # Arguments
    /// * `client`: An instance of your type that implements `Paginated`.
    /// * `params`: The initial parameters to fetch the first page.
    pub fn new(paginated: T, params: T::Params) -> Self {
        Self {
            client: paginated,
            params: Some(params),
            buffer: VecDeque::new(),
            request: None,
        }
    }
}

impl<T> Stream for PagingStream<T>
where
    T: Paginated,
    T: Unpin,
    T::Item: Unpin,
    T::Params: Unpin,
{
    type Item = Result<T::Item, T::Error>;

    fn poll_next(
        self: Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> Poll<Option<Self::Item>> {
        let slf = self.get_mut();

        loop {
            // #1: yield results from the buffer until exhaustion
            if let Some(value) = slf.buffer.pop_front() {
                return Poll::Ready(Some(Ok(value)));
            }

            if let Some(mut request) = slf.request.take() {
                match Pin::as_mut(&mut request).poll(cx) {
                    Poll::Ready(Ok((values, params))) => {
                        // #2: assign the returned values if the request was successful
                        slf.buffer.extend(values);
                        slf.params = params;
                        continue;
                    }
                    Poll::Ready(Err(err)) => {
                        // #3: yield the error if the request failed
                        return Poll::Ready(Some(Err(err)));
                    }
                    Poll::Pending => {
                        // #4: yield pending if the request is pending
                        slf.request = Some(request);
                        return Poll::Pending;
                    }
                }
            }

            if let Some(params) = slf.params.take() {
                // #5: send a new request if:
                //      1. there are no items in the buffer
                //      2. there is no pending request
                //      3. there are params
                slf.request = Some(Box::pin(slf.client.fetch_page(params)));
                cx.waker().wake_by_ref();
                return Poll::Pending;
            } else {
                // #6: yield None when there is nothing left to do
                return Poll::Ready(None);
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use futures::StreamExt;
    use std::sync::Once;
    use std::time;

    static INITIALIZE_TRACING: Once = Once::new();

    fn init_tracing() {
        INITIALIZE_TRACING.call_once(|| {
            tracing_subscriber::fmt()
                .with_test_writer()
                .with_max_level(tracing::Level::DEBUG)
                .init();
        });
    }

    pub struct Repository;

    pub struct Params {
        since: usize,
        until: usize,
        limit: usize,
    }

    impl Repository {
        fn get_next_page_params(params: &Params, results: &[usize]) -> Option<Params> {
            results.last().map(|last| Params {
                since: last + 1,
                until: params.until,
                limit: params.limit,
            })
        }
    }

    const END_OF_COLLECTION: usize = 1000;
    const ERR_RANGE_START: usize = 200;
    const ERR_RANGE_END: usize = 500;

    impl Paginated for Repository {
        type Params = Params;
        type Item = usize;
        type Error = ();

        fn fetch_page(
            &self,
            params: Self::Params,
        ) -> impl Future<Output = Result<(Vec<Self::Item>, Option<Self::Params>), Self::Error>>
        + Send
        + 'static {
            async move {
                tracing::debug!(message="Fetching page", since=?params.since, until=?params.until, limit=?params.limit);

                tokio::time::sleep(time::Duration::from_millis(5)).await;

                let mut values = Vec::with_capacity(params.limit);

                // return empty vec if since is larger than the end of the collection
                if params.since > END_OF_COLLECTION {
                    return Ok((values, None));
                }

                // return err if since is in the error range
                if params.since > ERR_RANGE_START && params.since < ERR_RANGE_END {
                    return Err(());
                }

                let requested_until = std::cmp::min(params.since + params.limit, params.until);

                let end_of_page = std::cmp::min(requested_until, END_OF_COLLECTION);

                for i in params.since..end_of_page {
                    values.push(i)
                }

                let params = Self::get_next_page_params(&params, &values);

                Ok((values, params))
            }
        }
    }

    #[tokio::test]
    async fn it_streams_up_until() {
        let mut since = 500;
        let until = 700;
        let limit = 100;

        let mut stream = PagingStream::new(
            Repository,
            Params {
                since,
                until,
                limit,
            },
        );

        let mut last_value = 0;
        while let Some(value) = stream.next().await {
            assert_eq!(value, Ok(since));
            last_value = value.unwrap();
            since += 1;
        }

        assert_eq!(last_value, until - 1);

        // subsequent polls yield None
        let value = stream.next().await;
        assert_eq!(value, None);
    }

    #[tokio::test]
    async fn it_terminates_at_the_end_of_the_collection() {
        init_tracing();

        let mut since = 900;
        let until = 1100;
        let limit = 100;

        let mut stream = PagingStream::new(
            Repository,
            Params {
                since,
                until,
                limit,
            },
        );

        let mut last_value = 0;
        while let Some(value) = stream.next().await {
            assert_eq!(value, Ok(since));
            last_value = value.unwrap();
            since += 1;
        }

        assert_eq!(last_value, END_OF_COLLECTION - 1);

        // subsequent polls yield None
        let value = stream.next().await;
        assert_eq!(value, None);
    }

    #[tokio::test]
    async fn it_streams_mutliples_of_limit() {
        init_tracing();

        let mut since = 0;
        let until = 20;
        let limit = 10;

        let mut stream = PagingStream::new(
            Repository,
            Params {
                since,
                until,
                limit,
            },
        );

        let mut last_value = 0;
        while let Some(value) = stream.next().await {
            assert_eq!(value, Ok(since));
            last_value = value.unwrap();
            since += 1;
        }

        assert_eq!(last_value, until - 1);

        // subsequent polls yield None
        let value = stream.next().await;
        assert_eq!(value, None);
    }

    #[tokio::test]
    async fn it_terminates_if_the_collection_is_empty() {
        init_tracing();

        let since = 1000;
        let until = 1001;
        let limit = 1;

        let mut stream = PagingStream::new(
            Repository,
            Params {
                since,
                until,
                limit,
            },
        );

        let value = stream.next().await;
        assert_eq!(value, None);
    }

    #[tokio::test]
    async fn it_terminates_if_limit_is_zero() {
        init_tracing();

        let since = 0;
        let until = 20;
        let limit = 0;

        let mut stream = PagingStream::new(
            Repository,
            Params {
                since,
                until,
                limit,
            },
        );

        let value = stream.next().await;
        assert_eq!(value, None);
    }

    #[tokio::test]
    async fn it_bails_out_on_error() {
        init_tracing();

        let since = 499;
        let until = 500;
        let limit = 1;

        let mut stream = PagingStream::new(
            Repository,
            Params {
                since,
                until,
                limit,
            },
        );

        // it yields the encountered error
        let value = stream.next().await;
        assert_eq!(value, Some(Err(())));

        // it then terminates
        let value = stream.next().await;
        assert_eq!(value, None);
    }

    #[tokio::test]
    async fn it_bails_out_on_error_for_a_subsequent_page() {
        init_tracing();

        let since = 200;
        let until = 201;
        let limit = 1;

        let mut stream = PagingStream::new(
            Repository,
            Params {
                since,
                until,
                limit,
            },
        );

        // it yield valid values from the first page
        let value = stream.next().await;
        assert_eq!(value, Some(Ok(200)));

        // it then yields the encountered error
        let value = stream.next().await;
        assert_eq!(value, Some(Err(())));

        // it then terminates
        let value = stream.next().await;
        assert_eq!(value, None);
    }
}
