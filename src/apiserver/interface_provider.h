// The MIT License (MIT)
//
// Copyright (c) 2015
//
// Permission is hereby granted, free of charge, to any person obtaining a copy
//     of this software and associated documentation files (the "Software"), to deal
// in the Software without restriction, including without limitation the rights
//     to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
//     copies of the Software, and to permit persons to whom the Software is
// furnished to do so, subject to the following conditions:
//
// The above copyright notice and this permission notice shall be included in all
//     copies or substantial portions of the Software.
//
// THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
// IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
// FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
//     AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
// LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
// OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
// SOFTWARE.

#pragma once
#include <functional>
#include <memory>
#include <string>
#include <unordered_map>
#include <vector>

#include "brpc/http_method.h"  // HttpMethod
#include "butil/iobuf.h"       // IOBuf
#include "proto/http.pb.h"

namespace fedb {
namespace http {

class PathPart;

/**
 *  Url holder struct.
 */
struct Url {
    // not parseable yet
    std::string scheme;
    std::string user;
    std::string password;
    std::string host;
    uint32_t port;

    // read by reduced url parser
    std::string url;
    std::string fragment;
    std::string path;
    std::unordered_map<std::string, std::string> query;

    std::vector<std::unique_ptr<PathPart>> parsePath(bool disableIds = false) const;
};

enum class PathType { PARAMETER, STRING };

class PathPart {
 public:
    virtual std::string getValue() const = 0;
    virtual PathType getType() const = 0;

    virtual ~PathPart() = default;
};

class PathParameter : public PathPart {
 public:
    PathParameter(std::string id);

    std::string getValue() const override;
    PathType getType() const override;
    std::string getId() const;
    void setValue(std::string const& value);

 private:
    std::string value_;
    std::string id_;
};

class PathString : public PathPart {
 public:
    PathString(std::string value);

    std::string getValue() const override;
    PathType getType() const override;

 private:
    std::string value_;
};

/**
 *  This parser starts with /path ... and not with the scheme.
 *  Because our Rest InterfaceProvider needs it that way.
 */
class ReducedUrlParser {
 public:
    /**
     *  parses a urlString to an url object.
     */
    static Url parse(std::string const& urlString);

 private:
    static void parseQuery(std::string const& query, Url& url);
};

class InterfaceProvider {
 public:
    InterfaceProvider() = default;
    InterfaceProvider& operator=(InterfaceProvider const&) = delete;
    InterfaceProvider(InterfaceProvider const&) = delete;

    typedef std::unordered_map<std::string, std::string> Params;
    using func = void(const Params& params, const butil::IOBuf& req_body, http::Response* resp);
    /**
     *  Registers a new get request handler.
     *
     *  @param path The url to listen on. The syntax of is quite complex and documented elsewhere.
     *  @param callback The function called when a client sends a request on the url.
     *
     */
    InterfaceProvider& get(std::string const& path, std::function<func> callback);

    /**
     *  Registers a new put request handler.
     *
     *  @param path The url to listen on. The syntax of is quite complex and documented elsewhere.
     *  @param callback The function called when a client sends a request on the url.
     *
     */
    InterfaceProvider& put(std::string const& path, std::function<func> callback);

    /**
     *  Registers a new post request handler.
     *
     *  @param path The url to listen on. The syntax of is quite complex and documented elsewhere.
     *  @param callback The function called when a client sends a request on the url.
     *
     */
    InterfaceProvider& post(std::string const& path, std::function<func> callback);

    bool handle(const std::string& path, const brpc::HttpMethod& method, const butil::IOBuf& req_body,
                http::Response* resp);

 private:
    struct BuiltRequest {
        Url url;
        std::function<func> callback;
    };

    static bool matching(const Url& received, const Url& registered);
    std::unordered_map<std::string, std::string> extractParameters(const Url& received, const Url& registered);

 private:
    void registerRequest(brpc::HttpMethod, const std::string& url, std::function<func>&& callback);

 private:
    std::unordered_map<int, std::vector<BuiltRequest>> requests_;
};
}  // namespace http
}  // namespace fedb
