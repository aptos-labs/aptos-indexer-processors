//
//
//
// /// Helper function to set up and start Postgres container.
// async fn setup_postgres() -> (String, ContainerAsync<GenericImage>) {
//     let postgres_container = GenericImage::new("postgres", "14")
//         .with_exposed_port(5432.tcp())
//         .with_wait_for(WaitFor::message_on_stderr(
//             "database system is ready to accept connections",
//         ))
//         .with_env_var("POSTGRES_DB", "postgres")
//         .with_env_var("POSTGRES_USER", "postgres")
//         .with_env_var("POSTGRES_PASSWORD", "postgres")
//         .start()
//         .await
//         .expect("Postgres started");
//
//     let host = postgres_container.get_host().await.unwrap();
//     let port = postgres_container
//         .get_host_port_ipv4(5432)
//         .await
//         .unwrap();
//     let db_url = format!("postgres://postgres:postgres@{host}:{port}/postgres");
//
//     (db_url, postgres_container)
// }