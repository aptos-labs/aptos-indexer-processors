use aptos_protos::indexer::v1::{
    raw_data_server::{RawData, RawDataServer},
    GetTransactionsRequest, TransactionsResponse,
};
use futures::Stream;
use std::pin::Pin;
use tonic::{Request, Response, Status};

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
        let version = req.into_inner().starting_version.unwrap();
        println!("Received request for version: {}", version);
        // let transaction = self.transactions.iter().find(|t| {
        //     t.transactions.iter().any(|tx| {
        //         println!("Checking transaction version: {}", tx.version);
        //         tx.version == version
        //     })
        // });

        for t in &self.transactions {
            for tx in &t.transactions {
                println!("Available transaction version: {}", tx.version);
            }
        }

        let transaction = self.transactions.iter().find(|t| {
            t.transactions.iter().any(|tx| {
                println!("Checking transaction version: {}", tx.version);
                tx.version == version
            })
        });
        // // This wouldn't work with calling chain id 
        // if let Some(t) = transaction {
        //     let mut response = t.clone();
        //     response.chain_id = Some(self.chain_id); // Ensure chain_id is correctly set
        //     println!("Returning response with chain_id: {:?}", response.chain_id);
        //     let stream = futures::stream::iter(vec![Ok(response)]);
        //     Ok(Response::new(Box::pin(stream)))
        // } else {
        //     println!("Transaction not found, returning default response");
        //     return Err(Status::not_found("Transaction not found"));
        // }

        let result = match transaction {
            Some(t) => t.clone(),
            None => {
                // just return what we have in self.transactions
                println!("Transaction not found, returning first transaction");
                self.transactions[0].clone()
            },
        };
        
        // let mut response = transaction.clone();
        // response.chain_id = Some(self.chain_id); // Set chain_id in the response
        let mut response = result.clone();
        response.chain_id = Some(self.chain_id); // Set chain_id in the response
        println!("Returning response: {:?}", response.chain_id);
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
