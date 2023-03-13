#!/usr/bin/env node

/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

try {
    const main = require("main__")
    const { Buffer } = require('node:buffer')

    async function actionLoop() {
        process.stdin.resume(); // start reading from stdin
        process.stdin.on('data', (chunk) => {
            if (chunk.length != 0) {
                let topicLength = chunk.readInt8()
                let topic = chunk.slice(1, topicLength+1).toString()
                let payload = chunk.slice(1+topicLength).toString().trim()
                let result = ''
                try {
                    result = main(payload)
                } catch (err) {
                    let message = err.message || err.toString()
                    result = "error:" + message
                }
                let res = Buffer.from(result + "\n", 'utf-8')
                process.stdout.write(res)
            }

        });
    }
    actionLoop()
} catch (e) {
    if (e.code == "MODULE_NOT_FOUND") {
        console.error("zipped actions must contain either package.json or index.js at the root.")
    }
    console.error(e)
    process.exit(1)
}
