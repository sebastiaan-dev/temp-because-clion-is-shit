#include "read_fast_lanes.hpp"

#include <duckdb/main/extension_util.hpp>
#include <fls/connection.hpp>

namespace duckdb {

//-------------------------------------------------------------------
// Translations
//-------------------------------------------------------------------
static LogicalType TranslateType(fastlanes::DataType type) {
	switch (type) {
	case fastlanes::DataType::DOUBLE:
		return LogicalType::DOUBLE;
	case fastlanes::DataType::FLOAT:
		return LogicalType::FLOAT;
	case fastlanes::DataType::INT8:
		return LogicalType::TINYINT;
	case fastlanes::DataType::INT16:
		return LogicalType::SMALLINT;
	case fastlanes::DataType::INT32:
		return LogicalType::INTEGER;
	case fastlanes::DataType::INT64:
		return LogicalType::BIGINT;
	case fastlanes::DataType::UINT8:
	case fastlanes::DataType::UINT16:
	case fastlanes::DataType::UINT32:
	case fastlanes::DataType::UINT64:
		// Unsigned types map to DuckDB's unsigned bigint
		return LogicalType::UBIGINT;
	case fastlanes::DataType::STR:
	case fastlanes::DataType::FLS_STR:
		return LogicalType::VARCHAR;
	case fastlanes::DataType::BOOLEAN:
		return LogicalType::BOOLEAN;
	case fastlanes::DataType::DATE:
		return LogicalType::DATE;
	case fastlanes::DataType::BYTE_ARRAY:
		return LogicalType::BLOB;
	case fastlanes::DataType::LIST:
		// TODO
		return LogicalType::LIST(LogicalType::SQLNULL);
	case fastlanes::DataType::STRUCT:
		// TODO
		return LogicalType::STRUCT({});
	case fastlanes::DataType::MAP:
		// TODO
		return LogicalType::MAP(LogicalType::SQLNULL, LogicalType::SQLNULL);
	case fastlanes::DataType::FALLBACK:
		return LogicalType::VARCHAR;
	case fastlanes::DataType::INVALID:
	default:
		throw InternalException("TranslateType: column type is not supported");
	}
}

//-------------------------------------------------------------------
// Transformations
//-------------------------------------------------------------------
static void TransformSimpleType(fastlanes::col_pt &column, Vector &output, uint64_t offset) {
	std::visit(fastlanes::overloaded {[&](const fastlanes::up<fastlanes::TypedCol<double>> &typed_col) {
		                                  for (idx_t i = 0; i < 1024; ++i) {
			                                  output.SetValue(i, Value {typed_col->data[offset + i]});
		                                  }
	                                  },
	                                  [&](const auto &) {
		                                  // TODO
	                                  }},
	           column);
}

static void TransformType(fastlanes::col_pt &column, Vector &output, uint64_t offset) {
	std::visit(fastlanes::overloaded {[](const std::monostate &) {
		                                  // TODO
	                                  },
	                                  [&]<typename T>(const fastlanes::up<fastlanes::TypedCol<T>> &typed_col) {
		                                  TransformSimpleType(column, output, offset);
	                                  },
	                                  [&](const fastlanes::up<fastlanes::List> &list_col) {
		                                  // TODO
	                                  },
	                                  [&](const fastlanes::up<fastlanes::Struct> &struct_col) {
		                                  // TODO
	                                  },
	                                  [&](const auto &) {
		                                  // TODO
	                                  }},
	           column);
}

//-------------------------------------------------------------------
// Local
//-------------------------------------------------------------------
static unique_ptr<LocalTableFunctionState> LocalInitFn(ExecutionContext &context, TableFunctionInitInput &input,
                                                       GlobalTableFunctionState *global_state) {
	auto local_state = make_uniq<FastLanesReadLocalState>();
	auto &bind_data = input.bind_data->Cast<FastLanesReadBindData>();

	local_state->conn = fastlanes::Connection {};

	fastlanes::Reader &reader = local_state->conn.read_fls(bind_data.directory);
	local_state->row_group = reader.materialize();

	return local_state;
};

//-------------------------------------------------------------------
// Global
//-------------------------------------------------------------------
static unique_ptr<GlobalTableFunctionState> GlobalInitFn(ClientContext &context, TableFunctionInitInput &input) {
	auto global_state = make_uniq<FastLanesReadGlobalState>();
	auto &bind_data = input.bind_data->Cast<FastLanesReadBindData>();

	global_state->n_vector = bind_data.n_vector;
	global_state->cur_vector = 0;

	return global_state;
};

//-------------------------------------------------------------------
// Bind
//-------------------------------------------------------------------
static unique_ptr<FunctionData> BindFn(ClientContext &context, TableFunctionBindInput &input,
                                       vector<LogicalType> &return_types, vector<string> &names) {
	auto read_bind_data = make_uniq<FastLanesReadBindData>();
	read_bind_data->directory = input.inputs[0].ToString();

	// Set up a connection to the file
	fastlanes::Connection conn;
	const fastlanes::Reader &fls_reader = conn.reset().read_fls(read_bind_data->directory);

	const auto footer = fls_reader.footer();
	const auto column_descriptors = footer.GetColumnDescriptors();
	// Fill the data types and return types.
	for (idx_t i = 0; i < column_descriptors.size(); ++i) {
		return_types.push_back(TranslateType(column_descriptors[i].data_type));
		names.push_back(column_descriptors[i].name);
	}

	read_bind_data->n_vector = footer.GetNVectors();

	return read_bind_data;
};

//-------------------------------------------------------------------
// Table
//-------------------------------------------------------------------
static void TableFn(ClientContext &context, TableFunctionInput &data, DataChunk &output) {
	auto &global_state = data.global_state->Cast<FastLanesReadGlobalState>();
	const auto &local_state = data.local_state->Cast<FastLanesReadLocalState>();

	if (global_state.cur_vector < global_state.n_vector) {
		// Buffer is full, call table function again for next batch of data.
		output.SetCardinality(1024);
		++global_state.cur_vector;
	} else {
		// This stops the stream of data, table function is no longer called.
		output.SetCardinality(0);
		return;
	}

	// ColumnCount is defined during the bind of the table function.
	for (idx_t col_idx = 0; col_idx < output.ColumnCount(); col_idx++) {
		auto &target_col = output.data[col_idx];
		auto &col = local_state.row_group->internal_rowgroup[col_idx];

		TransformType(col, target_col, global_state.cur_vector * 1024);
	}

	output.Verify();
}

//-------------------------------------------------------------------
// Register
//-------------------------------------------------------------------
void ReadFastLanes::Register(DatabaseInstance &db) {
	auto table_function = TableFunction("read_fls", {LogicalType::VARCHAR}, TableFn, BindFn, GlobalInitFn, LocalInitFn);
	// TODO: support
	// table_function.projection_pushdown = true;
	ExtensionUtil::RegisterFunction(db, table_function);
}
} // namespace duckdb
