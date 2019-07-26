#!/usr/bin/env bash
# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#   http://www.apache.org/licenses/LICENSE-2.0
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

if [ "$TRAVIS_PULL_REQUEST" == 'false' ]; then
  if [ "$TRAVIS_BRANCH" = 'master' ] || [ -n "$TRAVIS_TAG" ]; then
    openssl aes-256-cbc -K $encrypted_f2615f64684a_key -iv $encrypted_f2615f64684a_iv -in travis/codesigning.asc.enc -out travis/codesigning.asc -d
    gpg --fast-import travis/codesigning.asc
    mvn deploy -P sign --settings travis/mvnsettings.xml -Drat.skip -DskipTests
  fi
fi
