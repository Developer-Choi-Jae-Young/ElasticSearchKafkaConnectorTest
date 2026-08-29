# 🚀 ElasticSearch & Kafka Connector R&D Project

본 프로젝트는 **RDB(MySQL) - Kafka - Kafka Connect - Elasticsearch** 간의 실시간 데이터 파이프라인(CDC 기반 실시간 데이터 동기화) 구축 및 **한글 자소/초성 검색(AutoComplete)** 구현을 검증하는 **R&D 프로젝트**입니다.

---

## 📌 목차
1. [핵심 개념 및 도입 배경 (Why?)](#-핵심-개념-및-도입-배경-why)
   - [1. Elasticsearch (엘라스틱서치)](#1-elasticsearch-엘라스틱서치)
   - [2. Apache Kafka (아파치 카프카)](#2-apache-kafka-아파치-카프카)
   - [3. Kafka Connect & Debezium CDC](#3-kafka-connect--debezium-cdc)
   - [4. 기술 조합 도입 이유 및 이점](#4-기술-조합-도입-이유-및-이점)
2. [전체 파이프라인 아키텍처 및 Write/Read 분리 구조 (CQRS 개념)](#-전체-파이프라인-아키텍처-및-writeread-분리-구조)
3. [한글 자소 분석기(Jaso Tokenizer) 동작 원리](#-한글-자소-분석기jaso-tokenizer-동작-원리)
4. [Kafka Connect 커넥터 상세 설정 (Source & Sink JSON)](#-kafka-connect-커넥터-상세-설정-source--sink-json)
   - [1. Debezium MySQL Source Connector 등록](#1-debezium-mysql-source-connector-등록)
   - [2. Elasticsearch Sink Connector 등록](#2-elasticsearch-sink-connector-등록)
5. [프로젝트 구조 및 주요 코드 설명](#-프로젝트-구조-및-주요-코드-설명)
6. [환경 구축 및 실행 가이드](#-환경-구축-및-실행-가이드)
7. [API 명세 및 테스트](#-api-명세-및-테스트)
8. [자주 발생하는 이슈 및 트러블슈팅](#-자주-발생하는-이슈-및-트러블슈팅)

---

## 💡 핵심 개념 및 도입 배경 (Why?)

### 1. Elasticsearch (엘라스틱서치)
* **개념**: Apache Lucene 기반의 **분산 실시간 RESTful 검색 및 분석 엔진**입니다. JSON 도큐먼트 형태로 데이터를 저장하며 **역색인(Inverted Index)** 구조를 활용합니다.
* **핵심 특징**:
  * **역색인(Inverted Index)**: 텍스트의 각 단어(Token)가 어느 문서(Document)에 등장하는지 인덱싱하여, 대용량 데이터에서도 `LIKE '%키워드%'` 방식과 달리 **수 ms 내 풀텍스트 검색**을 수행합니다.
  * **커스텀 분석기(Custom Analyzer)**: 텍스트 저장 시 **자소 분리(초성 extraction)** 및 **N-gram 토큰화**를 조합해 "홍길동" -> "ㅎ", "홍ㄱ", "홍길" 검색을 완벽 지원합니다.

---

### 2. Apache Kafka (아파치 카프카)
* **개념**: 고성능 실시간 데이터 스트리밍 처리를 위한 **분산 이벤트 스트리밍 플랫폼(Message Broker)**입니다.
* **핵심 특징**:
  * **Pub/Sub 기반 시스템 결합도 제거(Decoupling)**: 데이터 생산업체(Producer)와 소비자(Consumer)가 완전히 분리되어, ES 인덱싱이 지연되거나 ES 서버가 점검 중이라도 **RDB 트랜잭션 및 API 서비스는 영향을 받지 않고 정상 작동**합니다.
  * **디스크 기반 영속성(Persistence) & 메시지 재처리**: 메시지를 디스크에 내구성 있게 저장하며 파티션을 통해 소비 상태(Offset)를 관리하므로 장애 복구 후 원활한 데이터 재처리가 가능합니다.

---

### 3. Kafka Connect & Debezium CDC
* **개념**: 
  * **Kafka Connect**: 외부 시스템(RDB, ES, S3 등)과 Kafka 간 데이터 입출력을 코딩 없이 **JSON 커넥터 설정**만으로 처리하는 프레임워크입니다.
  * **Debezium (CDC - Change Data Capture)**: MySQL의 Binlog(바이너리 로그)를 실시간 감지하여 DB의 `INSERT`, `UPDATE`, `DELETE` 이벤트를 캡처하여 Kafka Topic으로 발행하는 대표적인 CDC 솔루션입니다.
* **핵심 특징**:
  * **SMT (Single Message Transform)**: Debezium이 추출한 복잡한 CDC 래퍼 구조(`before`, `after`, `op` 등)에서 실제 변경된 데이터(`after`)만 깔끔한 JSON으로 변환하여 Elasticsearch로 전달합니다.

---

### 4. 기술 조합 도입 이유 및 이점

| 구분 | 전통적인 Dual Write 방식 | CDC + Kafka + Kafka Connect + Elasticsearch 방식 |
| :--- | :--- | :--- |
| **작동 방식** | 애플리케이션 서비스 로직에서 RDB 저장 후 ES API 직접 호출 | 애플리케이션은 DB에만 저장 $\rightarrow$ Binlog 캡처 CDC가 Kafka 거쳐 ES에 자동 반영 |
| **트랜잭션/안정성** | ES 장애 시 DB 저장도 같이 실패하거나 데이터 불일치 발생 | DB 저장과 ES 색인이 완전히 분리되어 한쪽 장애가 타 시스템으로 전파되지 않음 |
| **개발 생산성** | 모든 CUD API마다 ES 저장/수정/삭제 로직을 중복 구현해야 함 | DB에 CUD만 발생하면 **자동 이중화 파이프라인**으로 알아서 동기화됨 |
| **검색 성능** | RDB `LIKE '%키워드%'` (Full Table Scan, 초성 검색 불가) | ES 역색인 + Jaso Tokenizer로 **초성 검색, 오타 검색, 초고속 자동완성** 제공 |

---

## 🏗️ 전체 파이프라인 아키텍처 및 Write/Read 분리 구조

본 프로젝트의 비즈니스 로직(`UserService.java`)은 **Write(CUD) 작업**과 **Read(Search) 작업**의 대상 저장소가 다르게 분리된 **CQRS(Command Query Responsibility Segregation) 맛보기 형태**를 띱니다.

```
 [Client / REST API Request]
   │
   ├─► CUD 요청 (POST /user/insert, update, delete) 
   │    │
   │    ▼ [Write Path]
   │   ┌─────────────────────────┐
   │   │ UserService (JPA Save)  │
   │   └────────────┬────────────┘
   │                │ 1. INSERT / UPDATE / DELETE
   │                ▼
   │   ┌─────────────────────────┐
   │   │ MySQL RDB (user 테이블)  │
   │   └────────────┬────────────┘
   │                │ 2. Row-level Binlog (ROW/FULL)
   │                ▼
   │   ┌────────────────────────────────────────────────────────┐
   │   │ Debezium MySQL Source Connector (Port 8083)            │
   │   └────────────┬───────────────────────────────────────────┘
   │                │ 3. Produce JSON Event (Topic: mysql.test.user)
   │                ▼
   │   ┌────────────────────────────────────────────────────────┐
   │   │ Apache Kafka Cluster (Broker: 9092, 29092)              │
   │   └────────────┬───────────────────────────────────────────┘
   │                │ 4. Consume Event (SMT Unwrap: after payload)
   │                ▼
   │   ┌────────────────────────────────────────────────────────┐
   │   │ Confluent Elasticsearch Sink Connector (Port 8083)     │
   │   └────────────┬───────────────────────────────────────────┘
   │                │ 5. Auto Indexing / Document Sync
   │                ▼
   │   ┌────────────────────────────────────────────────────────┐
   │   │ Elasticsearch 8.17.2 (Index: user)                     │
   │   └────────────▲───────────────────────────────────────────┘
   │                │
   └─► Read 요청 (GET /user/select/all, match/{name})
        │
        └───────► [Read Path] (Fast Jaso/Text Search via UserDocRepository)
```

---

## 🔤 한글 자소 분석기(Jaso Tokenizer) 동작 원리

`user-settings.json` 및 `user-mapping.json`에서 정의한 Elasticsearch 인덱스 분석기는 한글 검색 핵심 역량입니다.

### 1. 색인 시점 (Index Time - `suggest_index_analyzer`)
사용자가 이름 **`"홍길동"`**을 입력하고 저장하면 다음과 같이 인덱싱됩니다.
1. **자소 분해 (`jaso_index_tokenizer`)**: `"홍길동"` $\rightarrow$ `"ㅎㅗㅇㄱㅣㄹㄷㅗㅇ"` (초성, 중성, 종성 완전 분해)
2. **N-gram 토큰화 (`suggest_filter`, min_gram: 2, max_gram: 50)**:
   - 2글자 자소: `"ㅎㅗ"`, `"ㅗㅇ"`, `"ㅇㄱ"`, `"ㄱㅣ"` ...
   - 3글자 자소: `"ㅎㅗㅇ"`, `"ㅗㅇㄱ"`, `"ㅇㄱㅣ"` ...
   - 초성 결합 조각 및 단계별 자소 조각들이 도큐먼트 역색인 테이블에 토큰으로 등록됨.

### 2. 검색 시점 (Search Time - `suggest_search_analyzer`)
사용자가 검색창에 **`"ㅎㄱ"`** (초성 검색) 또는 **`"홍길"`**을 입력하면:
1. 검색어 `"ㅎㄱ"` $\rightarrow$ `jaso_search_tokenizer`에 의해 `"ㅎㄱ"` 자소 추출.
2. 역색인 테이블에서 해당 자소 토큰을 가진 도큐먼트를 즉시 매칭하여 **"홍길동"**을 반환!

---

## ⚙️ Kafka Connect 커넥터 상세 설정 (Source & Sink JSON)

Kafka Connect Docker 컨테이너가 실행된 후, REST API(`http://localhost:8083/connectors`)를 통해 커넥터를 등록해야 전체 파이프라인이 구동됩니다.

### 1. Debezium MySQL Source Connector 등록
MySQL Binlog를 읽어 Kafka Topic(`mysql.test.user`)으로 전송합니다.

```bash
curl -X POST http://localhost:8083/connectors \
  -H "Content-Type: application/json" \
  -d '{
    "name": "mysql-source-connector",
    "config": {
      "connector.class": "io.debezium.connector.mysql.MySqlConnector",
      "tasks.max": "1",
      "database.hostname": "cjy951213.iptime.org",
      "database.port": "25500",
      "database.user": "root",
      "database.password": "root",
      "database.server.id": "223344",
      "database.server.name": "mysql",
      "database.include.list": "test",
      "table.include.list": "test.user",
      "database.history.kafka.bootstrap.servers": "broker:29092",
      "database.history.kafka.topic": "schema-changes.user",
      "key.converter": "org.apache.kafka.connect.json.JsonConverter",
      "key.converter.schemas.enable": "false",
      "value.converter": "org.apache.kafka.connect.json.JsonConverter",
      "value.converter.schemas.enable": "false",
      "transforms": "unwrap",
      "transforms.unwrap.type": "io.debezium.transforms.ExtractNewRecordState",
      "transforms.unwrap.drop.tombstones": "false"
    }
  }'
```

### 2. Elasticsearch Sink Connector 등록
Kafka Topic(`mysql.test.user`)의 메시지를 소비하여 Elasticsearch 인덱스(`user`)로 인덱싱합니다.

```bash
curl -X POST http://localhost:8083/connectors \
  -H "Content-Type: application/json" \
  -d '{
    "name": "elasticsearch-sink-connector",
    "config": {
      "connector.class": "io.confluent.connect.elasticsearch.ElasticsearchSinkConnector",
      "tasks.max": "1",
      "topics": "mysql.test.user",
      "key.ignore": "false",
      "schema.ignore": "true",
      "connection.url": "http://elasticsearch1:9200",
      "type.name": "_doc",
      "name.key.template": "${key}",
      "key.converter": "org.apache.kafka.connect.json.JsonConverter",
      "key.converter.schemas.enable": "false",
      "value.converter": "org.apache.kafka.connect.json.JsonConverter",
      "value.converter.schemas.enable": "false",
      "transforms": "ExtractKey",
      "transforms.ExtractKey.type": "org.apache.kafka.connect.transforms.ExtractField$Key",
      "transforms.ExtractKey.field": "id"
    }
  }'
```

---

## 📁 프로젝트 구조 및 주요 코드 설명

```
ElasticSearchKafkaConnectorTest
├── src/main/java/org/example/elasticsearchtest
│   ├── controller/MainController.java             # CUD 및 ES 조회 API 컨트롤러
│   ├── document/UserDocument.java               # ES 'user' 인덱스 도큐먼트 맵핑
│   ├── dto/                                     # Request / Response DTO 모음
│   ├── entity/UserEntity.java                   # MySQL 'user' 테이블 JPA 엔티티
│   ├── repository/
│   │   ├── document/UserDocRepository.java     # ES 전용 Repository (@Query 한글 매칭)
│   │   └── jpa/UserJPARepository.java           # MySQL 전용 JPA Repository
│   └── service/UserService.java                # CUD (MySQL) & Search (ES) 핵심 로직
└── src/main/resources
    ├── application.properties               # DB (MySQL) 및 ES 접속 정보 설정
    ├── docker/docker_command.txt            # Docker 컨테이너 생성/실행 가이드
    ├── elasticsearch/
    │   ├── mappings/user-mapping.json       # ES 필드 타입 및 analyzer 설정 JSON
    │   ├── plugin/install_command.txt       # Jaso 플러그인 & MySQL binlog 커맨드
    │   └── settings/user-settings.json      # Jaso 토크나이저 & N-gram 필터 정의
    └── jars/                                # Connect용 커넥터 JAR 라이브러리
```

### 🔑 `UserService.java` 구조적 유의사항
`UserService.java` 코드를 보면, `insertUser`, `updateUser`, `deleteUser` 작업 시 **ES Repository(`userDocRepository`)를 직접 호출하지 않습니다**.
* **오직 `userJPARepository`에만 저장/수정/삭제**합니다.
* 데이터가 MySQL에 저장되는 순간 Binlog $\rightarrow$ Debezium $\rightarrow$ Kafka $\rightarrow$ Kafka Connect $\rightarrow$ Elasticsearch 순서로 **비동기 이중화**되기 때문에 애플리케이션에서 ES 이중 작성을 할 필요가 없습니다!
* 조회(`selectUserAll`, `selectUserNameMatch`) 시에는 **`userDocRepository`를 통해서 ES 인덱스에서 초고속으로 조회**합니다.

---

## 🛠️ 환경 구축 및 실행 가이드

### Step 1. Docker 컨테이너 구동
`docker_command.txt` 파일 내 명령어를 사용하여 네트워크 및 인프라 컨테이너를 올립니다.

1. **Docker Network 생성**: `docker network create myela`, `docker network create my-network`
2. **Zookeeper & Kafka Broker 실행**: Port 2181, 9092, 29092
3. **Kafka UI 실행**: `http://localhost:9797` 접속하여 토픽 상태 확인
4. **Kafka Connect 실행**: Port 8083 (`jars` 디렉토리 마운트 필수)
5. **Elasticsearch 8.17.2 & Kibana 실행**: Port 9200, 5601

### Step 2. MySQL Binlog (CDC) 설정
`/etc/mysql/conf.d/binlog.cnf`에 다음 설정을 넣고 MySQL을 재시작합니다:
```ini
[mysqld]
server-id = 223344
log_bin = mysql-bin
binlog_format = ROW
binlog_row_image = FULL
expire_logs_days = 7
```

### Step 3. Elasticsearch 한글 분석기 설치 및 인덱스 세팅
1. **Jaso 플러그인 복사 및 설치**:
   ```bash
   docker cp ./jaso-analyzer-plugin-8.17.2-plugin.zip elasticsearch1:/usr/share/elasticsearch/
   docker exec -it elasticsearch1 bin/elasticsearch-plugin install file:///usr/share/elasticsearch/jaso-analyzer-plugin-8.17.2-plugin.zip
   docker restart elasticsearch1
   ```
2. **인덱스 생성 및 분석기 설정**:
   - `PUT http://localhost:9200/user` (`user-settings.json` 전달)
   - `PUT http://localhost:9200/user/_mapping` (`user-mapping.json` 전달)

### Step 4. Kafka Connect 커넥터 REST API 등록
상단의 **[Kafka Connect 커넥터 상세 설정]**의 `curl` 명령어를 실행하여 Source & Sink 커넥터를 연결합니다.

---

## 📡 API 명세 및 테스트

| Method | Endpoint | Description | Target Store |
| :--- | :--- | :--- | :--- |
| `POST` | `/user/insert` | 신규 사용자 저장 (`{userId, name, password}`) | **MySQL (JPA)** |
| `POST` | `/user/update` | 사용자 정보 수정 | **MySQL (JPA)** |
| `POST` | `/user/delete/{no}` | 사용자 삭제 | **MySQL (JPA)** |
| `GET` | `/user/select/all` | 전체 사용자 도큐먼트 조회 | **Elasticsearch** |
| `GET` | `/user/select/match/{name}` | 초성/한글 검색 (예: `name="ㅎㄱ"`) | **Elasticsearch** |

---

## 🚨 자주 발생하는 이슈 및 트러블슈팅

1. **Kafka Connect에서 Elasticsearch로 전송 시 Schema Error 발생**:
   - Debezium의 기본 출력 메시지에는 스키마 정보가 포함되어 ES 커넥터가 읽지 못할 수 있습니다.
   - `value.converter.schemas.enable: false` 설정 및 `ExtractNewRecordState` SMT를 적용해 순수 JSON 데이터만 전달하도록 해야 합니다.
2. **Elasticsearch에서 초성 검색이 동작하지 않는 경우**:
   - ES 인덱스 생성을 미리 하지 않고 Connector가 자동으로 `user` 인덱스를 생성한 경우, 기본 standard analyzer가 적용됩니다.
   - 반드시 **미리 `user-settings.json` 및 `user-mapping.json`을 사용하여 ES 인덱스를 수동 작성**한 뒤 파이프라인을 실행해야 합니다.
3. **Docker Network 분리 문제**:
   - `elasticsearch1`은 `myela` 네트워크에, `broker`/`kafka-connector`는 `my-network`에 속한 경우 통신이 되지 않을 수 있습니다. `docker network connect my-network elasticsearch1` 명령으로 네트워크를 묶어주어야 합니다.

---
