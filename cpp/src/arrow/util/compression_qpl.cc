// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

#include "arrow/util/compression_internal.h"

#include <cstddef>
#include <cstdint>
#include <memory>

#include <qpl/qpl.h>

#include "arrow/result.h"
#include "arrow/status.h"
#include "arrow/util/logging.h"
#include "arrow/util/macros.h"

namespace arrow {
namespace util {
namespace internal {

namespace {

// ----------------------------------------------------------------------
// qpl implementation

constexpr int kQPLMinCompressionLevel = 1;
constexpr int kQPLMaxCompressionLevel = 9;

static bool static_compression_initialized=false;
static qpl_job *job_pool[10];




// ----------------------------------------------------------------------
// qpl decompressor implementation

class QPLDecompressor : public Decompressor {
 public:
  explicit QPLDecompressor(qpl_path_t execution_path):
       initialized_(false), finished_(false),job_(NULL), execution_path_ (execution_path) {}

  ~QPLDecompressor() override {}


  Status Init() {
    DCHECK(!initialized_);
    finished_ = false;
    qpl_status status;

    uint32_t size;
    status = qpl_get_job_size(execution_path_, &size);
    if (status != QPL_STS_OK) {
        throw std::runtime_error("An error acquired during job size getting.");
    }
    uint8_t *job_buffer=new uint8_t[size];
    //job_buffer = std::make_unique<uint8_t[]>(size);
    job_ = reinterpret_cast<qpl_job *>(job_buffer);
    status = qpl_init_job(execution_path_, job_);
    if (status != QPL_STS_OK) {
        throw std::runtime_error("An error acquired during compression job initializing.");
        //return Status::OK();
    }else{
      initialized_=true;
      return Status::OK();
    }
  }

  Status Reset() override {
    DCHECK(initialized_);
    finished_ = false;
    //int ret;
    return Status::OK();
  }
  Result<DecompressResult> Decompress(int64_t input_len, const uint8_t* input,
                                      int64_t output_len, uint8_t* output) override {

    job_->op=qpl_op_decompress;
    job_->next_in_ptr=const_cast<uint8_t*>(input);
    job_->next_out_ptr=output;
    job_->available_in=input_len;
    job_->available_out=output_len;
    job_->flags=QPL_FLAG_FIRST | QPL_FLAG_LAST;

    //decompression
    qpl_status status = qpl_execute_job(job_);
    if (status != QPL_STS_OK) {
        throw std::runtime_error("Error while decompression occurred.");
    }else{
      return Status::OK();
    }

  }

  bool IsFinished() override { return finished_; }

 protected:
  bool initialized_;
  bool finished_;
  qpl_job* job_;
  qpl_path_t execution_path_;
};

// ----------------------------------------------------------------------
// qpl compressor implementation

class QPLCompressor : public Compressor {
 public:
  explicit QPLCompressor(int compression_level, qpl_path_t execution_path)
      : initialized_(false), finished_(false), job_(NULL),execution_path_(execution_path), compression_level_(compression_level) {
      }

  ~QPLCompressor() override {}

  Status Init() {
    DCHECK(!initialized_);
    finished_ = false;
    qpl_status status;
    uint32_t size;
    status = qpl_get_job_size(execution_path_, &size);
    if (status != QPL_STS_OK) {
        throw std::runtime_error("An error acquired during job size getting.");
    }
    uint8_t *job_buffer=new uint8_t[size];
    //job_buffer = std::make_unique<uint8_t[]>(size);
    job_ = reinterpret_cast<qpl_job *>(job_buffer);
    status = qpl_init_job(execution_path_, job_);
    if (status != QPL_STS_OK) {
        throw std::runtime_error("An error acquired during compression job initializing.");
        //return Status::OK();
    }else{
      initialized_=true;
      return Status::OK();
    }
  }

  Result<CompressResult> Compress(int64_t input_len, const uint8_t* input,
                                  int64_t output_len, uint8_t* output) override {
    DCHECK(initialized_) << "Called on non-initialized stream";

    job_->op=qpl_op_compress;
    job_->level=(qpl_compression_levels)compression_level_;
    job_->next_in_ptr=const_cast<uint8_t*>(input);
    job_->next_out_ptr=output;
    job_->available_in=input_len;
    job_->available_out=output_len;
    job_->flags = QPL_FLAG_FIRST | QPL_FLAG_LAST | QPL_FLAG_DYNAMIC_HUFFMAN | QPL_FLAG_OMIT_VERIFY;


    //decompression
    qpl_status status = qpl_execute_job(job_);
    if (status != QPL_STS_OK) {
        throw std::runtime_error("Error while compression occurred.");
    }else{
      return Status::OK();
    }
  }

  Result<FlushResult> Flush(int64_t output_len, uint8_t* output) override {
    DCHECK(initialized_) << "Called on non-initialized stream";


    //job_->next_in_ptr=input;
    job_->next_out_ptr=output;
    job_->available_in=0;
    job_->available_out=output_len;
    job_->flags = QPL_FLAG_FIRST | QPL_FLAG_LAST | QPL_FLAG_DYNAMIC_HUFFMAN | QPL_FLAG_OMIT_VERIFY;

    qpl_status status = qpl_execute_job(job_);

    int64_t bytes_written;
    if (status != QPL_STS_OK) {
      bytes_written = 0;
      throw std::runtime_error("Error while decompression occurred.");
    }else{
      bytes_written = output_len - job_->available_out;
    }
    // "If deflate returns with avail_out == 0, this function must be called
    //  again with the same value of the flush parameter and more output space
    //  (updated avail_out), until the flush is complete (deflate returns
    //  with non-zero avail_out)."
    // "Note that Z_BUF_ERROR is not fatal, and deflate() can be called again
    //  with more input and more output space to continue compressing."
    return FlushResult{bytes_written, job_->available_out == 0};
  }

  Result<EndResult> End(int64_t output_len, uint8_t* output) override {
    DCHECK(initialized_) << "Called on non-initialized stream";


    job_->next_out_ptr=output;
    job_->available_in=0;
    job_->available_out=output_len;
    job_->flags = QPL_FLAG_FIRST | QPL_FLAG_LAST | QPL_FLAG_DYNAMIC_HUFFMAN | QPL_FLAG_OMIT_VERIFY;

    qpl_status status = qpl_execute_job(job_);

    int64_t bytes_written = output_len -job_->available_out;
    if (status != QPL_STS_OK) {
      bytes_written = 0;
      throw std::runtime_error("Error while decompression occurred.");
      //return EndResult{bytes_written, false};
    }else{
      bytes_written = output_len - job_->available_out;
    }
      // Not everything could be flushed,
    return EndResult{bytes_written, true};
  }

 protected:
  bool initialized_;
  bool finished_;
  qpl_job* job_;
  qpl_path_t execution_path_;
  int compression_level_;
};

// ----------------------------------------------------------------------
// qpl codec implementation

class QPLCodec : public Codec {
 public:
  explicit QPLCodec(int compression_level, qpl_path_t execution_path)
      : compressor_initialized_(false),
        decompressor_initialized_(false),
        job_(NULL),
        execution_path_(execution_path) {
    compression_level_ = compression_level == kUseDefaultCompressionLevel
                             ? kQPLDefaultCompressionLevel
                             : compression_level;
  }

  ~QPLCodec() override {
    //EndCompressor();
    //EndDecompressor();
  }

  Result<std::shared_ptr<Compressor>> MakeCompressor() override {
    auto ptr = std::make_shared<QPLCompressor>(compression_level_,execution_path_);
    RETURN_NOT_OK(ptr->Init());
    return ptr;
  }

  Result<std::shared_ptr<Decompressor>> MakeDecompressor() override {
    auto ptr = std::make_shared<QPLDecompressor>(execution_path_);
    RETURN_NOT_OK(ptr->Init());
    return ptr;
  }

  Status InitCompressor() {
    DCHECK(!compressor_initialized_);
    if(!static_compression_initialized){
    //finished_ = false;
      qpl_status status;
      uint32_t size;
      status = qpl_get_job_size(execution_path_, &size);
      if (status != QPL_STS_OK) {
          throw std::runtime_error("An error acquired during job size getting.");
      }
      //std::unique_ptr<uint8_t[]> 
      uint8_t *job_buffer=new uint8_t[size];
      //job_buffer = std::make_unique<uint8_t[]>(size);
      job_pool[0] = reinterpret_cast<qpl_job *>(job_buffer);
      status = qpl_init_job(execution_path_, job_pool[0]);
      if (status != QPL_STS_OK) {
          throw std::runtime_error("An error acquired during compression job initializing.");
          //return Status::OK();
      }else{
        job_=job_pool[0];
        compressor_initialized_=true;
        static_compression_initialized=true;
        return Status::OK();
      }
    }else{
        job_=job_pool[0];
        compressor_initialized_=true;
        static_compression_initialized=true;
        return Status::OK();
    }
  }

  void EndCompressor() {
    if(compressor_initialized_){
      qpl_status status = qpl_fini_job(job_);
      if (status != QPL_STS_OK) {
          throw std::runtime_error("An error acquired during job finalization.");
      }
      compressor_initialized_ = false;
    }
  }

  Status InitDecompressor() {
    DCHECK(!decompressor_initialized_);
    //finished_ = false;
    qpl_status status;
    uint32_t size;
    status = qpl_get_job_size(execution_path_, &size);
    if (status != QPL_STS_OK) {
        throw std::runtime_error("An error acquired during job size getting.");
    }
    uint8_t *job_buffer=new uint8_t[size];
    //job_buffer = std::make_unique<uint8_t[]>(size);
    job_ = reinterpret_cast<qpl_job *>(job_buffer);
    status = qpl_init_job(execution_path_, job_);
    if (status != QPL_STS_OK) {
        throw std::runtime_error("An error acquired during compression job initializing.");
        //return Status::OK();
    }else{
      decompressor_initialized_=true;
      return Status::OK();
    }
  }

  void EndDecompressor() {
    if (decompressor_initialized_) {
      qpl_status status = qpl_fini_job(job_);
      if (status != QPL_STS_OK) {
          throw std::runtime_error("An error acquired during job finalization.");
      }
      decompressor_initialized_ = false;
    }
  }

  Result<int64_t> Decompress(int64_t input_length, const uint8_t* input,
                             int64_t output_buffer_length, uint8_t* output) override {
    if (!compressor_initialized_) {
      RETURN_NOT_OK(InitCompressor());
    }
    if (output_buffer_length == 0) {
      // The zlib library does not allow *output to be NULL, even when
      // output_buffer_length is 0 (inflate() will return Z_STREAM_ERROR). We don't
      // consider this an error, so bail early if no output is expected. Note that we
      // don't signal an error if the input actually contains compressed data.
      return 0;
    }

    // Reset the stream for this block
    job_->op=qpl_op_decompress;
    job_->next_in_ptr=const_cast<uint8_t*>(input);
    job_->next_out_ptr=output;
    job_->available_in=input_length;
    job_->available_out=output_buffer_length;
    job_->flags=QPL_FLAG_FIRST | QPL_FLAG_LAST;

    //decompression
    qpl_status status = qpl_execute_job(job_);
    if (status != QPL_STS_OK) {
        throw std::runtime_error("Error while decompression occurred.");
    }else{
      return job_->total_out;
    }

  }

  int64_t MaxCompressedLen(int64_t input_length,
                           const uint8_t* ARROW_ARG_UNUSED(input)) override {
    // Must be in compression mode

    return input_length+input_length/255+16;
  }

  Result<int64_t> Compress(int64_t input_length, const uint8_t* input,
                           int64_t output_buffer_len, uint8_t* output) override {
     DCHECK(compressor_initialized_) << "Called on non-initialized stream";

    job_->op=qpl_op_compress;
    job_->level=(qpl_compression_levels)compression_level_;
    job_->next_in_ptr=const_cast<uint8_t*>(input);
    job_->next_out_ptr=output;
    job_->available_in=input_length;
    job_->available_out=output_buffer_len;
    job_->flags = QPL_FLAG_FIRST | QPL_FLAG_LAST | QPL_FLAG_DYNAMIC_HUFFMAN | QPL_FLAG_OMIT_VERIFY ;


    //compression
    qpl_status status = qpl_execute_job(job_);
    if (status != QPL_STS_OK) {
        throw std::runtime_error("Error while compression occurred.");
    }
    // Actual output length
    return  job_->total_out;
  }

   Status Init() override {
    if(!compressor_initialized_){
      const Status init_compressor_status = InitCompressor();

   // if (init_compressor_status.ok()) {
      return init_compressor_status;
   }
   return Status::OK();
    //}
    //return InitDecompressor();
  } 

  Compression::type compression_type() const override { return Compression::QPL; }

  int compression_level() const override { return compression_level_; }
  int minimum_compression_level() const override { return kQPLMinCompressionLevel; }
  int maximum_compression_level() const override { return kQPLMaxCompressionLevel; }
  int default_compression_level() const override { return kQPLDefaultCompressionLevel; }

 private:  
  bool compressor_initialized_;
  bool decompressor_initialized_;
  qpl_job* job_;
  qpl_path_t execution_path_;
  int compression_level_;
};

}  // namespace

std::unique_ptr<Codec> MakeQPLCodec(int compression_level,  QPLPath::type execution_path ) {
  return std::unique_ptr<Codec>(new QPLCodec(compression_level,(qpl_path_t) execution_path));
}

  // namespace internal

}
}  // namespace util
}  // namespace arrow