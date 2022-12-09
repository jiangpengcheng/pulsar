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
    const main = require("./main__").process
    const readline = require('readline');
    const fs = require("fs")

    async function actionLoop() {
        process.stdin.setEncoding('utf8');
        process.stdout.setEncoding('utf8');
        const rl = readline.createInterface({
            input: process.stdin
        });
        for await (const line of rl) {
            try {
                let result = '';
                result = main(line)
                if (typeof result === 'undefined') {
                    result = '';
                }
                if (Promise.resolve(result) == result)
                    try {
                        result = await result
                    } catch (error) {
                        if (typeof error === 'undefined') {
                            error = {}
                        }
                        result = JSON.stringify({"error": error})
                    }
                process.stdout.write(result + "\n");
            } catch (err) {
                console.log(err);
                let message = err.message || err.toString()
                let error = { "error": message }
                process.stdout.write(JSON.stringify(error) + "\n");
            }
        }
    }
    actionLoop()
} catch (e) {
    if (e.code == "MODULE_NOT_FOUND") {
        console.log("zipped actions must contain either package.json or index.js at the root.")
    }
    console.log(e)
    process.exit(1)
}