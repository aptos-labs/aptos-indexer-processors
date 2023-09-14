import * as yaml from "js-yaml";
import * as fs from "fs";

/**
 * This class represents the basic configuration required for the worker to connect to
 * the txn stream and the database.
 */
class Config {
  constructor(
    public chain_id: bigint,
    public grpc_data_stream_endpoint: string,
    public grpc_data_stream_api_key: string,
    public starting_version: bigint,
    public db_connection_uri: string,
  ) {}

  public static from_yaml_file(path: string): Config {
    const contents = fs.readFileSync(path, "utf8");
    return yaml.load(contents) as Config;
  }
}

export { Config };
