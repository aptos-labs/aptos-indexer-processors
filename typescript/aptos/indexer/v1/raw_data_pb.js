// source: aptos/indexer/v1/raw_data.proto
/**
 * @fileoverview
 * @enhanceable
 * @suppress {missingRequire} reports error on implicit type usages.
 * @suppress {messageConventions} JS Compiler reports an error if a variable or
 *     field starts with 'MSG_' and isn't a translatable message.
 * @public
 */
// GENERATED CODE -- DO NOT EDIT!
/* eslint-disable */
// @ts-nocheck

var jspb = require('google-protobuf');
var goog = jspb;
var global = (function() {
  if (this) { return this; }
  if (typeof window !== 'undefined') { return window; }
  if (typeof global !== 'undefined') { return global; }
  if (typeof self !== 'undefined') { return self; }
  return Function('return this')();
}.call(null));

var aptos_transaction_testing1_v1_transaction_pb = require('../../../aptos/transaction/testing1/v1/transaction_pb.js');
goog.object.extend(proto, aptos_transaction_testing1_v1_transaction_pb);
goog.exportSymbol('proto.aptos.indexer.v1.GetTransactionsRequest', null, global);
goog.exportSymbol('proto.aptos.indexer.v1.TransactionsResponse', null, global);
/**
 * Generated by JsPbCodeGenerator.
 * @param {Array=} opt_data Optional initial data array, typically from a
 * server response, or constructed directly in Javascript. The array is used
 * in place and becomes part of the constructed object. It is not cloned.
 * If no data is provided, the constructed object will be empty, but still
 * valid.
 * @extends {jspb.Message}
 * @constructor
 */
proto.aptos.indexer.v1.GetTransactionsRequest = function(opt_data) {
  jspb.Message.initialize(this, opt_data, 0, -1, null, null);
};
goog.inherits(proto.aptos.indexer.v1.GetTransactionsRequest, jspb.Message);
if (goog.DEBUG && !COMPILED) {
  /**
   * @public
   * @override
   */
  proto.aptos.indexer.v1.GetTransactionsRequest.displayName = 'proto.aptos.indexer.v1.GetTransactionsRequest';
}
/**
 * Generated by JsPbCodeGenerator.
 * @param {Array=} opt_data Optional initial data array, typically from a
 * server response, or constructed directly in Javascript. The array is used
 * in place and becomes part of the constructed object. It is not cloned.
 * If no data is provided, the constructed object will be empty, but still
 * valid.
 * @extends {jspb.Message}
 * @constructor
 */
proto.aptos.indexer.v1.TransactionsResponse = function(opt_data) {
  jspb.Message.initialize(this, opt_data, 0, -1, proto.aptos.indexer.v1.TransactionsResponse.repeatedFields_, null);
};
goog.inherits(proto.aptos.indexer.v1.TransactionsResponse, jspb.Message);
if (goog.DEBUG && !COMPILED) {
  /**
   * @public
   * @override
   */
  proto.aptos.indexer.v1.TransactionsResponse.displayName = 'proto.aptos.indexer.v1.TransactionsResponse';
}



if (jspb.Message.GENERATE_TO_OBJECT) {
/**
 * Creates an object representation of this proto.
 * Field names that are reserved in JavaScript and will be renamed to pb_name.
 * Optional fields that are not set will be set to undefined.
 * To access a reserved field use, foo.pb_<name>, eg, foo.pb_default.
 * For the list of reserved names please see:
 *     net/proto2/compiler/js/internal/generator.cc#kKeyword.
 * @param {boolean=} opt_includeInstance Deprecated. whether to include the
 *     JSPB instance for transitional soy proto support:
 *     http://goto/soy-param-migration
 * @return {!Object}
 */
proto.aptos.indexer.v1.GetTransactionsRequest.prototype.toObject = function(opt_includeInstance) {
  return proto.aptos.indexer.v1.GetTransactionsRequest.toObject(opt_includeInstance, this);
};


/**
 * Static version of the {@see toObject} method.
 * @param {boolean|undefined} includeInstance Deprecated. Whether to include
 *     the JSPB instance for transitional soy proto support:
 *     http://goto/soy-param-migration
 * @param {!proto.aptos.indexer.v1.GetTransactionsRequest} msg The msg instance to transform.
 * @return {!Object}
 * @suppress {unusedLocalVariables} f is only used for nested messages
 */
proto.aptos.indexer.v1.GetTransactionsRequest.toObject = function(includeInstance, msg) {
  var f, obj = {
    startingVersion: jspb.Message.getFieldWithDefault(msg, 1, 0),
    transactionsCount: jspb.Message.getFieldWithDefault(msg, 2, 0)
  };

  if (includeInstance) {
    obj.$jspbMessageInstance = msg;
  }
  return obj;
};
}


/**
 * Deserializes binary data (in protobuf wire format).
 * @param {jspb.ByteSource} bytes The bytes to deserialize.
 * @return {!proto.aptos.indexer.v1.GetTransactionsRequest}
 */
proto.aptos.indexer.v1.GetTransactionsRequest.deserializeBinary = function(bytes) {
  var reader = new jspb.BinaryReader(bytes);
  var msg = new proto.aptos.indexer.v1.GetTransactionsRequest;
  return proto.aptos.indexer.v1.GetTransactionsRequest.deserializeBinaryFromReader(msg, reader);
};


/**
 * Deserializes binary data (in protobuf wire format) from the
 * given reader into the given message object.
 * @param {!proto.aptos.indexer.v1.GetTransactionsRequest} msg The message object to deserialize into.
 * @param {!jspb.BinaryReader} reader The BinaryReader to use.
 * @return {!proto.aptos.indexer.v1.GetTransactionsRequest}
 */
proto.aptos.indexer.v1.GetTransactionsRequest.deserializeBinaryFromReader = function(msg, reader) {
  while (reader.nextField()) {
    if (reader.isEndGroup()) {
      break;
    }
    var field = reader.getFieldNumber();
    switch (field) {
    case 1:
      var value = /** @type {number} */ (reader.readUint64());
      msg.setStartingVersion(value);
      break;
    case 2:
      var value = /** @type {number} */ (reader.readUint64());
      msg.setTransactionsCount(value);
      break;
    default:
      reader.skipField();
      break;
    }
  }
  return msg;
};


/**
 * Serializes the message to binary data (in protobuf wire format).
 * @return {!Uint8Array}
 */
proto.aptos.indexer.v1.GetTransactionsRequest.prototype.serializeBinary = function() {
  var writer = new jspb.BinaryWriter();
  proto.aptos.indexer.v1.GetTransactionsRequest.serializeBinaryToWriter(this, writer);
  return writer.getResultBuffer();
};


/**
 * Serializes the given message to binary data (in protobuf wire
 * format), writing to the given BinaryWriter.
 * @param {!proto.aptos.indexer.v1.GetTransactionsRequest} message
 * @param {!jspb.BinaryWriter} writer
 * @suppress {unusedLocalVariables} f is only used for nested messages
 */
proto.aptos.indexer.v1.GetTransactionsRequest.serializeBinaryToWriter = function(message, writer) {
  var f = undefined;
  f = /** @type {number} */ (jspb.Message.getField(message, 1));
  if (f != null) {
    writer.writeUint64(
      1,
      f
    );
  }
  f = /** @type {number} */ (jspb.Message.getField(message, 2));
  if (f != null) {
    writer.writeUint64(
      2,
      f
    );
  }
};


/**
 * optional uint64 starting_version = 1;
 * @return {number}
 */
proto.aptos.indexer.v1.GetTransactionsRequest.prototype.getStartingVersion = function() {
  return /** @type {number} */ (jspb.Message.getFieldWithDefault(this, 1, 0));
};


/**
 * @param {number} value
 * @return {!proto.aptos.indexer.v1.GetTransactionsRequest} returns this
 */
proto.aptos.indexer.v1.GetTransactionsRequest.prototype.setStartingVersion = function(value) {
  return jspb.Message.setField(this, 1, value);
};


/**
 * Clears the field making it undefined.
 * @return {!proto.aptos.indexer.v1.GetTransactionsRequest} returns this
 */
proto.aptos.indexer.v1.GetTransactionsRequest.prototype.clearStartingVersion = function() {
  return jspb.Message.setField(this, 1, undefined);
};


/**
 * Returns whether this field is set.
 * @return {boolean}
 */
proto.aptos.indexer.v1.GetTransactionsRequest.prototype.hasStartingVersion = function() {
  return jspb.Message.getField(this, 1) != null;
};


/**
 * optional uint64 transactions_count = 2;
 * @return {number}
 */
proto.aptos.indexer.v1.GetTransactionsRequest.prototype.getTransactionsCount = function() {
  return /** @type {number} */ (jspb.Message.getFieldWithDefault(this, 2, 0));
};


/**
 * @param {number} value
 * @return {!proto.aptos.indexer.v1.GetTransactionsRequest} returns this
 */
proto.aptos.indexer.v1.GetTransactionsRequest.prototype.setTransactionsCount = function(value) {
  return jspb.Message.setField(this, 2, value);
};


/**
 * Clears the field making it undefined.
 * @return {!proto.aptos.indexer.v1.GetTransactionsRequest} returns this
 */
proto.aptos.indexer.v1.GetTransactionsRequest.prototype.clearTransactionsCount = function() {
  return jspb.Message.setField(this, 2, undefined);
};


/**
 * Returns whether this field is set.
 * @return {boolean}
 */
proto.aptos.indexer.v1.GetTransactionsRequest.prototype.hasTransactionsCount = function() {
  return jspb.Message.getField(this, 2) != null;
};



/**
 * List of repeated fields within this message type.
 * @private {!Array<number>}
 * @const
 */
proto.aptos.indexer.v1.TransactionsResponse.repeatedFields_ = [1];



if (jspb.Message.GENERATE_TO_OBJECT) {
/**
 * Creates an object representation of this proto.
 * Field names that are reserved in JavaScript and will be renamed to pb_name.
 * Optional fields that are not set will be set to undefined.
 * To access a reserved field use, foo.pb_<name>, eg, foo.pb_default.
 * For the list of reserved names please see:
 *     net/proto2/compiler/js/internal/generator.cc#kKeyword.
 * @param {boolean=} opt_includeInstance Deprecated. whether to include the
 *     JSPB instance for transitional soy proto support:
 *     http://goto/soy-param-migration
 * @return {!Object}
 */
proto.aptos.indexer.v1.TransactionsResponse.prototype.toObject = function(opt_includeInstance) {
  return proto.aptos.indexer.v1.TransactionsResponse.toObject(opt_includeInstance, this);
};


/**
 * Static version of the {@see toObject} method.
 * @param {boolean|undefined} includeInstance Deprecated. Whether to include
 *     the JSPB instance for transitional soy proto support:
 *     http://goto/soy-param-migration
 * @param {!proto.aptos.indexer.v1.TransactionsResponse} msg The msg instance to transform.
 * @return {!Object}
 * @suppress {unusedLocalVariables} f is only used for nested messages
 */
proto.aptos.indexer.v1.TransactionsResponse.toObject = function(includeInstance, msg) {
  var f, obj = {
    transactionsList: jspb.Message.toObjectList(msg.getTransactionsList(),
    aptos_transaction_testing1_v1_transaction_pb.Transaction.toObject, includeInstance),
    chainId: jspb.Message.getFieldWithDefault(msg, 2, 0)
  };

  if (includeInstance) {
    obj.$jspbMessageInstance = msg;
  }
  return obj;
};
}


/**
 * Deserializes binary data (in protobuf wire format).
 * @param {jspb.ByteSource} bytes The bytes to deserialize.
 * @return {!proto.aptos.indexer.v1.TransactionsResponse}
 */
proto.aptos.indexer.v1.TransactionsResponse.deserializeBinary = function(bytes) {
  var reader = new jspb.BinaryReader(bytes);
  var msg = new proto.aptos.indexer.v1.TransactionsResponse;
  return proto.aptos.indexer.v1.TransactionsResponse.deserializeBinaryFromReader(msg, reader);
};


/**
 * Deserializes binary data (in protobuf wire format) from the
 * given reader into the given message object.
 * @param {!proto.aptos.indexer.v1.TransactionsResponse} msg The message object to deserialize into.
 * @param {!jspb.BinaryReader} reader The BinaryReader to use.
 * @return {!proto.aptos.indexer.v1.TransactionsResponse}
 */
proto.aptos.indexer.v1.TransactionsResponse.deserializeBinaryFromReader = function(msg, reader) {
  while (reader.nextField()) {
    if (reader.isEndGroup()) {
      break;
    }
    var field = reader.getFieldNumber();
    switch (field) {
    case 1:
      var value = new aptos_transaction_testing1_v1_transaction_pb.Transaction;
      reader.readMessage(value,aptos_transaction_testing1_v1_transaction_pb.Transaction.deserializeBinaryFromReader);
      msg.addTransactions(value);
      break;
    case 2:
      var value = /** @type {number} */ (reader.readUint64());
      msg.setChainId(value);
      break;
    default:
      reader.skipField();
      break;
    }
  }
  return msg;
};


/**
 * Serializes the message to binary data (in protobuf wire format).
 * @return {!Uint8Array}
 */
proto.aptos.indexer.v1.TransactionsResponse.prototype.serializeBinary = function() {
  var writer = new jspb.BinaryWriter();
  proto.aptos.indexer.v1.TransactionsResponse.serializeBinaryToWriter(this, writer);
  return writer.getResultBuffer();
};


/**
 * Serializes the given message to binary data (in protobuf wire
 * format), writing to the given BinaryWriter.
 * @param {!proto.aptos.indexer.v1.TransactionsResponse} message
 * @param {!jspb.BinaryWriter} writer
 * @suppress {unusedLocalVariables} f is only used for nested messages
 */
proto.aptos.indexer.v1.TransactionsResponse.serializeBinaryToWriter = function(message, writer) {
  var f = undefined;
  f = message.getTransactionsList();
  if (f.length > 0) {
    writer.writeRepeatedMessage(
      1,
      f,
      aptos_transaction_testing1_v1_transaction_pb.Transaction.serializeBinaryToWriter
    );
  }
  f = /** @type {number} */ (jspb.Message.getField(message, 2));
  if (f != null) {
    writer.writeUint64(
      2,
      f
    );
  }
};


/**
 * repeated aptos.transaction.testing1.v1.Transaction transactions = 1;
 * @return {!Array<!proto.aptos.transaction.testing1.v1.Transaction>}
 */
proto.aptos.indexer.v1.TransactionsResponse.prototype.getTransactionsList = function() {
  return /** @type{!Array<!proto.aptos.transaction.testing1.v1.Transaction>} */ (
    jspb.Message.getRepeatedWrapperField(this, aptos_transaction_testing1_v1_transaction_pb.Transaction, 1));
};


/**
 * @param {!Array<!proto.aptos.transaction.testing1.v1.Transaction>} value
 * @return {!proto.aptos.indexer.v1.TransactionsResponse} returns this
*/
proto.aptos.indexer.v1.TransactionsResponse.prototype.setTransactionsList = function(value) {
  return jspb.Message.setRepeatedWrapperField(this, 1, value);
};


/**
 * @param {!proto.aptos.transaction.testing1.v1.Transaction=} opt_value
 * @param {number=} opt_index
 * @return {!proto.aptos.transaction.testing1.v1.Transaction}
 */
proto.aptos.indexer.v1.TransactionsResponse.prototype.addTransactions = function(opt_value, opt_index) {
  return jspb.Message.addToRepeatedWrapperField(this, 1, opt_value, proto.aptos.transaction.testing1.v1.Transaction, opt_index);
};


/**
 * Clears the list making it empty but non-null.
 * @return {!proto.aptos.indexer.v1.TransactionsResponse} returns this
 */
proto.aptos.indexer.v1.TransactionsResponse.prototype.clearTransactionsList = function() {
  return this.setTransactionsList([]);
};


/**
 * optional uint64 chain_id = 2;
 * @return {number}
 */
proto.aptos.indexer.v1.TransactionsResponse.prototype.getChainId = function() {
  return /** @type {number} */ (jspb.Message.getFieldWithDefault(this, 2, 0));
};


/**
 * @param {number} value
 * @return {!proto.aptos.indexer.v1.TransactionsResponse} returns this
 */
proto.aptos.indexer.v1.TransactionsResponse.prototype.setChainId = function(value) {
  return jspb.Message.setField(this, 2, value);
};


/**
 * Clears the field making it undefined.
 * @return {!proto.aptos.indexer.v1.TransactionsResponse} returns this
 */
proto.aptos.indexer.v1.TransactionsResponse.prototype.clearChainId = function() {
  return jspb.Message.setField(this, 2, undefined);
};


/**
 * Returns whether this field is set.
 * @return {boolean}
 */
proto.aptos.indexer.v1.TransactionsResponse.prototype.hasChainId = function() {
  return jspb.Message.getField(this, 2) != null;
};


goog.object.extend(exports, proto.aptos.indexer.v1);
