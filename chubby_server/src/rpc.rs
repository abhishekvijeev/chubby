#[derive(Clone, PartialEq, ::prost::Message)]
pub struct CreateSessionRequest {}
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct CreateSessionResponse {
    #[prost(string, tag = "1")]
    pub session_id: ::prost::alloc::string::String,
    #[prost(uint64, tag = "2")]
    pub lease_length: u64,
}
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct DeleteSessionRequest {
    #[prost(string, tag = "1")]
    pub session_id: ::prost::alloc::string::String,
}
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct DeleteSessionResponse {
    #[prost(bool, tag = "1")]
    pub success: bool,
}
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct KeepAliveRequest {
    #[prost(string, tag = "1")]
    pub session_id: ::prost::alloc::string::String,
}
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct KeepAliveResponse {
    #[prost(uint64, tag = "1")]
    pub lease_length: u64,
}
#[doc = r" Generated client implementations."]
pub mod chubby_client {
    #![allow(unused_variables, dead_code, missing_docs, clippy::let_unit_value)]
    use tonic::codegen::*;
    #[derive(Debug, Clone)]
    pub struct ChubbyClient<T> {
        inner: tonic::client::Grpc<T>,
    }
    impl ChubbyClient<tonic::transport::Channel> {
        #[doc = r" Attempt to create a new client by connecting to a given endpoint."]
        pub async fn connect<D>(dst: D) -> Result<Self, tonic::transport::Error>
        where
            D: std::convert::TryInto<tonic::transport::Endpoint>,
            D::Error: Into<StdError>,
        {
            let conn = tonic::transport::Endpoint::new(dst)?.connect().await?;
            Ok(Self::new(conn))
        }
    }
    impl<T> ChubbyClient<T>
    where
        T: tonic::client::GrpcService<tonic::body::BoxBody>,
        T::ResponseBody: Body + Send + 'static,
        T::Error: Into<StdError>,
        <T::ResponseBody as Body>::Error: Into<StdError> + Send,
    {
        pub fn new(inner: T) -> Self {
            let inner = tonic::client::Grpc::new(inner);
            Self { inner }
        }
        pub fn with_interceptor<F>(
            inner: T,
            interceptor: F,
        ) -> ChubbyClient<InterceptedService<T, F>>
        where
            F: tonic::service::Interceptor,
            T: tonic::codegen::Service<
                http::Request<tonic::body::BoxBody>,
                Response = http::Response<
                    <T as tonic::client::GrpcService<tonic::body::BoxBody>>::ResponseBody,
                >,
            >,
            <T as tonic::codegen::Service<http::Request<tonic::body::BoxBody>>>::Error:
                Into<StdError> + Send + Sync,
        {
            ChubbyClient::new(InterceptedService::new(inner, interceptor))
        }
        #[doc = r" Compress requests with `gzip`."]
        #[doc = r""]
        #[doc = r" This requires the server to support it otherwise it might respond with an"]
        #[doc = r" error."]
        pub fn send_gzip(mut self) -> Self {
            self.inner = self.inner.send_gzip();
            self
        }
        #[doc = r" Enable decompressing responses with `gzip`."]
        pub fn accept_gzip(mut self) -> Self {
            self.inner = self.inner.accept_gzip();
            self
        }
        pub async fn create_session(
            &mut self,
            request: impl tonic::IntoRequest<super::CreateSessionRequest>,
        ) -> Result<tonic::Response<super::CreateSessionResponse>, tonic::Status> {
            self.inner.ready().await.map_err(|e| {
                tonic::Status::new(
                    tonic::Code::Unknown,
                    format!("Service was not ready: {}", e.into()),
                )
            })?;
            let codec = tonic::codec::ProstCodec::default();
            let path = http::uri::PathAndQuery::from_static("/rpc.chubby/CreateSession");
            self.inner.unary(request.into_request(), path, codec).await
        }
        pub async fn delete_session(
            &mut self,
            request: impl tonic::IntoRequest<super::DeleteSessionRequest>,
        ) -> Result<tonic::Response<super::DeleteSessionResponse>, tonic::Status> {
            self.inner.ready().await.map_err(|e| {
                tonic::Status::new(
                    tonic::Code::Unknown,
                    format!("Service was not ready: {}", e.into()),
                )
            })?;
            let codec = tonic::codec::ProstCodec::default();
            let path = http::uri::PathAndQuery::from_static("/rpc.chubby/DeleteSession");
            self.inner.unary(request.into_request(), path, codec).await
        }
        pub async fn keep_alive(
            &mut self,
            request: impl tonic::IntoRequest<super::KeepAliveRequest>,
        ) -> Result<tonic::Response<super::KeepAliveResponse>, tonic::Status> {
            self.inner.ready().await.map_err(|e| {
                tonic::Status::new(
                    tonic::Code::Unknown,
                    format!("Service was not ready: {}", e.into()),
                )
            })?;
            let codec = tonic::codec::ProstCodec::default();
            let path = http::uri::PathAndQuery::from_static("/rpc.chubby/KeepAlive");
            self.inner.unary(request.into_request(), path, codec).await
        }
    }
}
#[doc = r" Generated server implementations."]
pub mod chubby_server {
    #![allow(unused_variables, dead_code, missing_docs, clippy::let_unit_value)]
    use tonic::codegen::*;
    #[doc = "Generated trait containing gRPC methods that should be implemented for use with ChubbyServer."]
    #[async_trait]
    pub trait Chubby: Send + Sync + 'static {
        async fn create_session(
            &self,
            request: tonic::Request<super::CreateSessionRequest>,
        ) -> Result<tonic::Response<super::CreateSessionResponse>, tonic::Status>;
        async fn delete_session(
            &self,
            request: tonic::Request<super::DeleteSessionRequest>,
        ) -> Result<tonic::Response<super::DeleteSessionResponse>, tonic::Status>;
        async fn keep_alive(
            &self,
            request: tonic::Request<super::KeepAliveRequest>,
        ) -> Result<tonic::Response<super::KeepAliveResponse>, tonic::Status>;
    }
    #[derive(Debug)]
    pub struct ChubbyServer<T: Chubby> {
        inner: _Inner<T>,
        accept_compression_encodings: (),
        send_compression_encodings: (),
    }
    struct _Inner<T>(Arc<T>);
    impl<T: Chubby> ChubbyServer<T> {
        pub fn new(inner: T) -> Self {
            let inner = Arc::new(inner);
            let inner = _Inner(inner);
            Self {
                inner,
                accept_compression_encodings: Default::default(),
                send_compression_encodings: Default::default(),
            }
        }
        pub fn with_interceptor<F>(inner: T, interceptor: F) -> InterceptedService<Self, F>
        where
            F: tonic::service::Interceptor,
        {
            InterceptedService::new(Self::new(inner), interceptor)
        }
    }
    impl<T, B> tonic::codegen::Service<http::Request<B>> for ChubbyServer<T>
    where
        T: Chubby,
        B: Body + Send + 'static,
        B::Error: Into<StdError> + Send + 'static,
    {
        type Response = http::Response<tonic::body::BoxBody>;
        type Error = Never;
        type Future = BoxFuture<Self::Response, Self::Error>;
        fn poll_ready(&mut self, _cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
            Poll::Ready(Ok(()))
        }
        fn call(&mut self, req: http::Request<B>) -> Self::Future {
            let inner = self.inner.clone();
            match req.uri().path() {
                "/rpc.chubby/CreateSession" => {
                    #[allow(non_camel_case_types)]
                    struct CreateSessionSvc<T: Chubby>(pub Arc<T>);
                    impl<T: Chubby> tonic::server::UnaryService<super::CreateSessionRequest> for CreateSessionSvc<T> {
                        type Response = super::CreateSessionResponse;
                        type Future = BoxFuture<tonic::Response<Self::Response>, tonic::Status>;
                        fn call(
                            &mut self,
                            request: tonic::Request<super::CreateSessionRequest>,
                        ) -> Self::Future {
                            let inner = self.0.clone();
                            let fut = async move { (*inner).create_session(request).await };
                            Box::pin(fut)
                        }
                    }
                    let accept_compression_encodings = self.accept_compression_encodings;
                    let send_compression_encodings = self.send_compression_encodings;
                    let inner = self.inner.clone();
                    let fut = async move {
                        let inner = inner.0;
                        let method = CreateSessionSvc(inner);
                        let codec = tonic::codec::ProstCodec::default();
                        let mut grpc = tonic::server::Grpc::new(codec).apply_compression_config(
                            accept_compression_encodings,
                            send_compression_encodings,
                        );
                        let res = grpc.unary(method, req).await;
                        Ok(res)
                    };
                    Box::pin(fut)
                }
                "/rpc.chubby/DeleteSession" => {
                    #[allow(non_camel_case_types)]
                    struct DeleteSessionSvc<T: Chubby>(pub Arc<T>);
                    impl<T: Chubby> tonic::server::UnaryService<super::DeleteSessionRequest> for DeleteSessionSvc<T> {
                        type Response = super::DeleteSessionResponse;
                        type Future = BoxFuture<tonic::Response<Self::Response>, tonic::Status>;
                        fn call(
                            &mut self,
                            request: tonic::Request<super::DeleteSessionRequest>,
                        ) -> Self::Future {
                            let inner = self.0.clone();
                            let fut = async move { (*inner).delete_session(request).await };
                            Box::pin(fut)
                        }
                    }
                    let accept_compression_encodings = self.accept_compression_encodings;
                    let send_compression_encodings = self.send_compression_encodings;
                    let inner = self.inner.clone();
                    let fut = async move {
                        let inner = inner.0;
                        let method = DeleteSessionSvc(inner);
                        let codec = tonic::codec::ProstCodec::default();
                        let mut grpc = tonic::server::Grpc::new(codec).apply_compression_config(
                            accept_compression_encodings,
                            send_compression_encodings,
                        );
                        let res = grpc.unary(method, req).await;
                        Ok(res)
                    };
                    Box::pin(fut)
                }
                "/rpc.chubby/KeepAlive" => {
                    #[allow(non_camel_case_types)]
                    struct KeepAliveSvc<T: Chubby>(pub Arc<T>);
                    impl<T: Chubby> tonic::server::UnaryService<super::KeepAliveRequest> for KeepAliveSvc<T> {
                        type Response = super::KeepAliveResponse;
                        type Future = BoxFuture<tonic::Response<Self::Response>, tonic::Status>;
                        fn call(
                            &mut self,
                            request: tonic::Request<super::KeepAliveRequest>,
                        ) -> Self::Future {
                            let inner = self.0.clone();
                            let fut = async move { (*inner).keep_alive(request).await };
                            Box::pin(fut)
                        }
                    }
                    let accept_compression_encodings = self.accept_compression_encodings;
                    let send_compression_encodings = self.send_compression_encodings;
                    let inner = self.inner.clone();
                    let fut = async move {
                        let inner = inner.0;
                        let method = KeepAliveSvc(inner);
                        let codec = tonic::codec::ProstCodec::default();
                        let mut grpc = tonic::server::Grpc::new(codec).apply_compression_config(
                            accept_compression_encodings,
                            send_compression_encodings,
                        );
                        let res = grpc.unary(method, req).await;
                        Ok(res)
                    };
                    Box::pin(fut)
                }
                _ => Box::pin(async move {
                    Ok(http::Response::builder()
                        .status(200)
                        .header("grpc-status", "12")
                        .header("content-type", "application/grpc")
                        .body(empty_body())
                        .unwrap())
                }),
            }
        }
    }
    impl<T: Chubby> Clone for ChubbyServer<T> {
        fn clone(&self) -> Self {
            let inner = self.inner.clone();
            Self {
                inner,
                accept_compression_encodings: self.accept_compression_encodings,
                send_compression_encodings: self.send_compression_encodings,
            }
        }
    }
    impl<T: Chubby> Clone for _Inner<T> {
        fn clone(&self) -> Self {
            Self(self.0.clone())
        }
    }
    impl<T: std::fmt::Debug> std::fmt::Debug for _Inner<T> {
        fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            write!(f, "{:?}", self.0)
        }
    }
    impl<T: Chubby> tonic::transport::NamedService for ChubbyServer<T> {
        const NAME: &'static str = "rpc.chubby";
    }
}
