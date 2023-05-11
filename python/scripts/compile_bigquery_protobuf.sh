PATH_TO_BIGQUERY_MODELS_FOLDER=${PATH_TO_BIGQUERY_MODELS_FOLDER:-"../models"}
proto_file="$1"
protoc --proto_path=$PATH_TO_BIGQUERY_MODELS_FOLDER"/protos" --python_out=$PATH_TO_BIGQUERY_MODELS_FOLDER"/proto_autogen" --pyi_out=$PATH_TO_BIGQUERY_MODELS_FOLDER"/proto_autogen" ${proto_file}