import * as yaml from "js-yaml";
import * as fs from "fs";

class Config {
  constructor(
    public chain_id: number,
    public grpc_data_stream_endpoint: string,
    public grpc_data_stream_api_key: string,
    public starting_version: number,
    public db_connection_uri: string,
    public cursor_filename: string
  ) {}

  public static from_yaml_file(path: string): Config {
    const contents = fs.readFileSync(path, "utf8");
    return yaml.load(contents) as Config;
  }
}

export { Config };
