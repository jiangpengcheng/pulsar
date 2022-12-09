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

package main

import (
	"bufio"
	"fmt"
	"io"
	"log"
	"os"
	"reflect"
	"strings"
)

func main() {
	// input
	reader := bufio.NewReader(os.Stdin)

	// read-eval-print loop
	for {
		// read one line
		inbuf, err := reader.ReadBytes('\n')
		if err != nil {
			if err != io.EOF {
				log.Println(err)
			}
			break
		}
		// parse one line
		input := string(inbuf[:])

		funcMain := reflect.ValueOf(MainMethod)
		params := []reflect.Value{reflect.ValueOf(input)}
		reflectResult := funcMain.Call(params)
		result := reflectResult[0].Interface()
		// encode the answer
		output := fmt.Sprintf("%s", result)
		output = strings.Replace(output, "\n", "", -1)
		fmt.Fprintf(os.Stdout, "%s\n", output)
	}
}
