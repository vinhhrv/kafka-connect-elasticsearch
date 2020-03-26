/*
 * Copyright 2018 Confluent Inc.
 *
 * Licensed under the Confluent Community License (the "License"); you may not use
 * this file except in compliance with the License.  You may obtain a copy of the
 * License at
 *
 * http://www.confluent.io/confluent-community-license
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OF ANY KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations under the License.
 */

package io.confluent.connect.elasticsearch;

import java.io.IOException;
import java.util.Map;
import java.util.Objects;

import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.JsonSerializer;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializerProvider;
import com.fasterxml.jackson.databind.module.SimpleModule;

public class IndexableRecord {

  public final Key key;
  public String payload;
  public final Long version;

  public IndexableRecord(Key key, String payload, Long version) {
    this.key = key;
    this.version = version;
    this.payload = payload;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    IndexableRecord that = (IndexableRecord) o;
    return Objects.equals(key, that.key)
        && Objects.equals(payload, that.payload)
        && Objects.equals(version, that.version);
  }

  @Override
  public int hashCode() {
    return Objects.hash(key, version, payload);
  }

  public String getPayloadWithCamelConvert() {
    try {
      return jsonToCamelJson(payload);
    } catch (Exception ex) {
      return null;
    }
  }

  private String jsonToCamelJson(String jsonString) 
      throws IOException {
    SimpleModule simpleModule = new SimpleModule();
    simpleModule.addKeySerializer(String.class, new CamelCaseKeySerializer());
    ObjectMapper mapper = new ObjectMapper();
    mapper.registerModule(simpleModule);
    Map<String, Object> map = mapper.readValue(jsonString, 
                                              new TypeReference<Map<String, Object>>() {});
    return mapper.writeValueAsString(map);
  }

  private class CamelCaseKeySerializer extends JsonSerializer<String> {
    @Override
    public void serialize(String value, JsonGenerator gen, SerializerProvider serializers)
        throws IOException, JsonProcessingException {
      String key = Character.toLowerCase(value.charAt(0)) + value.substring(1);
      gen.writeFieldName(key);
    }
  }
}
