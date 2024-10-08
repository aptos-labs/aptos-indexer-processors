
use tonic::{Request, Response, Status};
use futures::Stream;
use aptos_protos::{
    indexer::v1::{
        raw_data_server::{RawData, RawDataServer},
        GetTransactionsRequest, TransactionsResponse,
    },
    transaction::v1::Transaction,
};
use std::pin::Pin;


#[derive(Default)]
pub struct MockGrpcServer {
    pub transactions: Vec<TransactionsResponse>,
    pub chain_id: u64,
}

type ResponseStream = Pin<Box<dyn Stream<Item = Result<TransactionsResponse, Status>> + Send>>;

#[tonic::async_trait]
impl RawData for MockGrpcServer {
    type GetTransactionsStream = ResponseStream;
    async fn get_transactions(
        &self,
        req: Request<GetTransactionsRequest>,
    ) -> Result<Response<Self::GetTransactionsStream>, Status> {
        type GetTransactionsStream = ResponseStream;

        let version = req.into_inner().starting_version.unwrap();
        let transaction = self
            .transactions
            .iter()
            .find(|t| t.transactions.first().unwrap().version == version)
            .unwrap();

        let mut response = transaction.clone();
        response.chain_id = Some(self.chain_id); // Set chain_id in the response

        let stream = futures::stream::iter(vec![Ok(response)]);
        Ok(Response::new(Box::pin(stream)))
    }
}


impl MockGrpcServer {
    pub async fn run(self) {
        tonic::transport::Server::builder()
            .add_service(
                RawDataServer::new(self)
                    .accept_compressed(tonic::codec::CompressionEncoding::Zstd)
                    .send_compressed(tonic::codec::CompressionEncoding::Zstd),
            )
            .serve("127.0.0.1:51254".parse().unwrap())
            .await
            .unwrap();
    }
}
