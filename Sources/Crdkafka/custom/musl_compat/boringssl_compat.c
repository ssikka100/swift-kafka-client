//===----------------------------------------------------------------------===//
//
// This source file is part of the swift-kafka-client open source project
//
// Copyright (c) 2026 Apple Inc. and the swift-kafka-client project authors
// Licensed under Apache License v2.0
//
// See LICENSE.txt for license information
// See CONTRIBUTORS.txt for the list of swift-kafka-client project authors
//
// SPDX-License-Identifier: Apache-2.0
//
//===----------------------------------------------------------------------===//

// The Swift Static Linux SDK (musl) bundles BoringSSL as its crypto library. BoringSSL omits a
// couple of OpenSSL functions that librdkafka references, so linking a static musl executable
// fails with undefined symbols. These thin shims provide them in terms of BoringSSL's API.
//
// Guarded to musl only (Linux without glibc); on glibc/macOS this file compiles to nothing and
// the platform's real OpenSSL is used.
#if defined(__linux__) && !defined(__GLIBC__)

#include <openssl/rand.h>
#include <openssl/ssl.h>
#include <openssl/x509.h>

// librdkafka's rdrand.c uses RAND_priv_bytes; BoringSSL only ships RAND_bytes.
int RAND_priv_bytes(unsigned char *buf, int num) {
    return RAND_bytes(buf, (size_t)num);
}

// librdkafka's rdkafka_ssl.c uses SSL_CTX_use_cert_and_key (OpenSSL 1.1.1+); BoringSSL omits it.
// Emulate it via the individual certificate/key/chain setters.
int SSL_CTX_use_cert_and_key(SSL_CTX *ctx, X509 *cert, EVP_PKEY *pkey,
                             STACK_OF(X509) *chain, int override) {
    (void)override;
    if (cert != NULL && SSL_CTX_use_certificate(ctx, cert) != 1) {
        return 0;
    }
    if (pkey != NULL && SSL_CTX_use_PrivateKey(ctx, pkey) != 1) {
        return 0;
    }
    if (chain != NULL) {
        for (size_t i = 0; i < sk_X509_num(chain); i++) {
            if (SSL_CTX_add1_chain_cert(ctx, sk_X509_value(chain, i)) != 1) {
                return 0;
            }
        }
    }
    return 1;
}

#endif
