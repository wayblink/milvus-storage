#include "common/arrow_util.h"
#include "common/log.h"
#include "common/macro.h"

namespace milvus_storage {
Result<std::shared_ptr<parquet::arrow::FileReader>> MakeArrowFileReader(std::shared_ptr<arrow::fs::FileSystem> fs,
                                                                        std::string& file_path) {
  ASSIGN_OR_RETURN_ARROW_NOT_OK(auto file, fs->OpenInputFile(file_path));

  std::unique_ptr<parquet::arrow::FileReader> file_reader;
  RETURN_ARROW_NOT_OK(parquet::arrow::OpenFile(file, arrow::default_memory_pool(), &file_reader));
  return std::shared_ptr(std::move(file_reader));
}

Result<std::shared_ptr<arrow::RecordBatchReader>> MakeArrowRecordBatchReader(
    std::shared_ptr<parquet::arrow::FileReader> reader, std::shared_ptr<ReadOptions> options) {
  auto metadata = reader->parquet_reader()->metadata();
  LOG_STORAGE_INFO_ << "metadata for record: " << metadata->schema()->ToString();
  std::vector<int> row_group_indices;
  std::vector<int> column_indices;

  // if (options->output_columns().size() == 0) {
  //   for (auto i = 0; i < metadata->schema()->num_columns(); i++) {
  //     auto column = metadata->schema()->Column(i);
  //   column_indices.emplace_back(column->logical_type)
  //   }
  // }
  for (const auto& column_name : options->columns) {
    auto column_idx = metadata->schema()->ColumnIndex(column_name);
    column_indices.emplace_back(column_idx);
  }
  for (const auto& filter : options->filters) {
    auto column_idx = metadata->schema()->ColumnIndex(filter->get_column_name());
    column_indices.emplace_back(column_idx);
  }

  for (int i = 0; i < metadata->num_row_groups(); ++i) {
    auto row_group_metadata = metadata->RowGroup(i);
    bool can_ignored = false;

    for (const auto& filter : options->filters) {
      auto column_idx = metadata->schema()->ColumnIndex(filter->get_column_name());
      auto column_meta = row_group_metadata->ColumnChunk(column_idx);
      auto stats = column_meta->statistics();

      if (stats == nullptr || !stats->HasMinMax()) {
        continue;
      }
      if (filter->CheckStatistics(stats.get())) {
        can_ignored = true;
        break;
      }
    }
    if (!can_ignored) {
      row_group_indices.emplace_back(i);
    }
  }
  std::shared_ptr<arrow::RecordBatchReader> record_reader;

  // RETURN_ARROW_NOT_OK(reader->GetRecordBatchReader(row_group_indices, column_indices, &record_reader));
  RETURN_ARROW_NOT_OK(reader->GetRecordBatchReader(row_group_indices, &record_reader));
  LOG_STORAGE_INFO_ << "metadata for batch reader:" << record_reader->schema()->ToString(true);
  return record_reader;
}

}  // namespace milvus_storage
