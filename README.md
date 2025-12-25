# CalciteRestAPIAdapter

**CalciteRestAPIAdapter** enables data retrieval from REST services using standard SQL syntax. It builds on the [Apache Calcite](https://calcite.apache.org/) framework, which allows the creation of adapters for diverse data sources through JDBC.
Settings are provided via OpenAPI specs for structure discovery and FreeMarker templates for customizing HTTP requests. This allows seamless SQL-to-REST mapping with support for DNF/CNF filters and query pushdown.



***

## Key Features

The adapter supports the following main capabilities:

- **Full SQL Support**: Execute standard SQL queries with SELECT, WHERE, ORDER BY, LIMIT, JOINs against REST APIs
- **Multiple Request/Response Formats**: Handle JSON, XML, and CSV for both requests and responses
- **Complex Nested Structures**: Flatten deeply nested JSON/XML structures (e.g., departments → employees → tasks) into relational tables
- **Advanced Filtering**: Support for Disjunctive Normal Form (DNF) and Conjunctive Normal Form (CNF) filter structures
- **Flexible Request Customization**: Customize request body, URL, and headers using FreeMarker templates
- **Dynamic Headers**: Generate HTTP headers dynamically based on query context and template variables
- **URL Parameters**: Pass filters, projections, and pagination as URL query parameters
- **Pagination Support**: Automatic handling of REST API pagination
- **Projection Pushdown**: Optimize queries by requesting only needed fields from REST API
- **Parameter Direction Control**: Handle REQUEST-only, RESPONSE-only, and BOTH parameters with automatic validation
- **Type Mapping**: Explicit control over JSON field to SQL column type mappings
- **OpenAPI Integration**: Define REST API behavior using standard OpenAPI specifications
- **Custom OpenAPI Extensions**: Use extensions like `x-calcite-restapi-*` for advanced configuration
- **Comprehensive Testing**: Extensive test coverage for all formats and features

***

## Test Coverage

The adapter includes comprehensive integration tests covering all major features. Each test validates specific functionality with concrete examples of SQL queries, HTTP requests, and expected behavior.

### Detailed Test Examples

#### 1. Projection Pushdown - [ProjectionsTest.java](src/test/java/org/apache/calcite/adapter/restapi/tests/ProjectionsTest.java)

**What it tests**: Verifies that only requested columns are sent to REST API, minimizing data transfer.

**Example 1**: Select specific columns with filter
```sql
SELECT name, last_name FROM users WHERE age >= 23
```
HTTP Request:
```json
{
  "page": 1,
  "limit": 100,
  "projections": ["name", "lastName"],
  "filters": [{"name": "age", "operator": ">=", "value": "23"}]
}
```
Expected: 5 users (Bob, Martin, Alice, John, Sarah)

**Example 2**: SELECT * sends all columns
```sql
SELECT * FROM users WHERE age >= 30
```
HTTP Request:
```json
{
  "projections": ["id", "name", "lastName", "age", "department"],
  "filters": [{"name": "age", "operator": ">=", "value": "30"}]
}
```
Expected: 2 users (Martin, John)

**Example 3**: Projections without filters
```sql
SELECT name, last_name FROM users
```
HTTP Request:
```json
{
  "projections": ["name", "lastName"]
}
```
Expected: All 7 users, but only name/last_name fields fetched

---

#### 2. Pagination Handling - [PaginationTest.java](src/test/java/org/apache/calcite/adapter/restapi/tests/PaginationTest.java)

**What it tests**: Automatic pagination with multiple page fetches until data exhausted.

**Example 1**: Fetch all data with small page size
```sql
SELECT * FROM users ORDER BY id
```
Configuration: `pageSize=2`, 7 users total

HTTP Requests (automatic):
```json
Request 1: {"page": 1, "limit": 2}  // Returns users 1-2
Request 2: {"page": 2, "limit": 2}  // Returns users 3-4
Request 3: {"page": 3, "limit": 2}  // Returns users 5-6
Request 4: {"page": 4, "limit": 2}  // Returns user 7
```
Expected: 4 requests, all 7 users fetched

**Example 2**: SQL LIMIT optimization
```sql
SELECT * FROM users ORDER BY id LIMIT 3
```
Configuration: `pageSize=2`

HTTP Requests:
```json
Request 1: {"page": 1, "limit": 2}  // Returns 2 users
Request 2: {"page": 2, "limit": 2}  // Returns 1 user (enough for LIMIT 3)
```
Expected: Stop after 2 requests, exactly 3 users

**Example 3**: Pagination stops on partial page
```
Page 1: 2 users (full page)
Page 2: 1 user (< pageSize) → STOP
```
Expected: No request for page 3

---

#### 3. SQL JOIN Operations - [JoinTest.java](src/test/java/org/apache/calcite/adapter/restapi/tests/JoinTest.java)

**What it tests**: INNER JOIN, SELF JOIN, CROSS JOIN with filter/projection pushdown.

**Example 1**: INNER JOIN with filter
```sql
SELECT u.id, u.name, u.last_name, u.department,
       d.id as dept_id, d.name as dept_name, d.budget
FROM company_schema.users u
JOIN company_schema.departments d ON u.department = d.name
WHERE d.budget >= 250000
ORDER BY u.id
```

HTTP Requests:
```json
// To /users endpoint:
{"page": 0, "limit": 100}  // No filter (filter on departments only)

// To /departments endpoint:
{
  "page": 0,
  "limit": 100,
  "filters_dnf": [[
    {"name": "budget", "operator": ">=", "value": "250000"}
  ]]
}
```

Expected: 6 users in high-budget departments (IT: Alice, Bob; Sales: Eve, Frank; Marketing: Grace; Finance: Henry)

**Example 2**: SELF JOIN to find colleagues
```sql
SELECT u1.name as user1, u2.name as user2, u1.department
FROM users u1
JOIN users u2 ON u1.department = u2.department
WHERE u1.id < u2.id
```
Expected: Colleague pairs (Alice & Bob in IT, Carol & David in HR, Eve & Frank in Sales)

**Example 3**: CROSS JOIN cartesian product
```sql
SELECT u1.name, u2.name
FROM users u1
CROSS JOIN users u2
WHERE u1.id < u2.id
LIMIT 10
```
Expected: First 10 user pairs from all possible combinations

**Example 4**: Projection pushdown in JOIN
```sql
SELECT u.name, d.name as dept_name, d.budget
FROM users u JOIN departments d ON u.department = d.name
LIMIT 5
```

HTTP Requests verify projections:
```json
// To /users:
{"projections": ["name", "department"]}  // department needed for JOIN condition

// To /departments:
{"projections": ["name", "budget"]}
```

---

#### 4. Filter Specification Variants - [AllVariantsIntegrationTest.java](src/test/java/org/apache/calcite/adapter/restapi/tests/AllVariantsIntegrationTest.java)

**What it tests**: 5 different HTTP request filter formats for the same SQL query.

**SQL Query** (same for all variants):
```sql
WHERE name='Alice' AND last_name='Smith' AND department='IT'
```

**Variant 1: Simple flat filters**
```json
{
  "page": 1,
  "limit": 100,
  "name": "Alice",
  "last_name": "Smith",
  "department": "IT"
}
```

**Variant 2: DNF with 'where' + 'or'** (for complex queries)
```sql
WHERE ((name='Bob' AND age>=21) OR (name='Alice' AND (department='IT' OR age>=25)))
  AND (last_name='Smith' OR (department='IT' AND age<30))
```
```json
{
  "where": [
    {"field": "name", "operator": "=", "value": "Bob"},
    {"field": "age", "operator": ">=", "value": 21}
  ],
  "or": [
    [
      {"field": "name", "operator": "=", "value": "Alice"},
      {"field": "department", "operator": "=", "value": "IT"}
    ],
    [
      {"field": "name", "operator": "=", "value": "Alice"},
      {"field": "age", "operator": ">=", "value": 25}
    ]
  ]
}
```

**Variant 3: CNF with 'where' + 'and'**
```json
{
  "where": [...],  // First OR group
  "and": [[...], [...]]  // Additional OR groups
}
```

**Variant 4: DNF with 'filters' array**
```json
{
  "filters": [
    [{"field": "name", "operator": "=", "value": "Bob"}],  // AND group 1
    [{"field": "age", "operator": ">=", "value": 21}]      // AND group 2
  ]
}
```

**Variant 5: CNF with 'filters' array**
```json
{
  "filters": [
    [{"field": "name", "operator": "=", "value": "Bob"}],  // OR group 1
    [{"field": "age", "operator": ">=", "value": 21}]      // OR group 2
  ]
}
```

All variants support the same complex SQL queries but with different HTTP request formats.

---

#### 5. Nested JSON Structures - [NestedStructureTest.java](src/test/java/org/apache/calcite/adapter/restapi/tests/NestedStructureTest.java)

**What it tests**: Flattening deeply nested JSON (departments → employees → tasks) into relational tables.

**Nested JSON Structure**:
```json
{
  "departments": [
    {
      "id": 1,
      "name": "IT",
      "location": "Building A",
      "budget": 500000,
      "employees": [
        {
          "id": 101,
          "name": "Alice",
          "position": "Developer",
          "salary": 80000,
          "tasks": [
            {"id": 1001, "title": "Fix bug #123", "status": "Done", "priority": "High"},
            {"id": 1002, "title": "Code review", "status": "In Progress", "priority": "Medium"}
          ]
        }
      ]
    }
  ]
}
```

**Flattened SQL Table**:
```
department_id | department_name | employee_id | employee_name | task_id | task_title      | task_status
1             | IT              | 101         | Alice         | 1001    | Fix bug #123    | Done
1             | IT              | 101         | Alice         | 1002    | Code review     | In Progress
...
```

**Example Queries**:

```sql
-- Query all flattened data
SELECT * FROM departments ORDER BY task_id
-- Expected: 5 rows (one per task)

-- Filter on middle level (employees)
SELECT * FROM departments WHERE employee_name = 'Alice'
-- Expected: 2 tasks (Alice's tasks)

-- Filter on deepest level (tasks)
SELECT * FROM departments WHERE task_status = 'In Progress'
-- Expected: 3 tasks with status "In Progress"

-- Aggregation across nested data
SELECT department_name, COUNT(*) as task_count
FROM departments
GROUP BY department_name
-- Expected: IT=4 tasks, HR=1 task
```

**Configuration** (x-calcite-restapi-mappings):
```yaml
- name: departments
  fields:
    - name: employees
      fields:
        - name: tasks
          array: true
```

---

#### 6. Response Structure Variants - [ResponseStructureTest.java](src/test/java/org/apache/calcite/adapter/restapi/tests/ResponseStructureTest.java)

**What it tests**: Different JSON/XML response formats.

**Structure 1: Anonymous Array** (direct array, no wrapper)
```json
[
  {"id": 1, "name": "Item 1", "value": "Value 1"},
  {"id": 2, "name": "Item 2", "value": "Value 2"},
  {"id": 3, "name": "Item 3", "value": "Value 3"}
]
```
Expected: 3 rows

**Structure 2: Single Object** (one-row table)
```json
{
  "id": 50,
  "name": "Standalone Item",
  "description": "This is a standalone item",
  "status": "active"
}
```
Expected: 1 row

**Structure 3: Fields with Array** (root fields repeat for each array item)
```json
{
  "response_id": 12345,
  "response_timestamp": "2023-12-20T10:00:00Z",
  "response_source": "product_catalog_api",
  "categories": [
    {
      "category_id": 1,
      "category_name": "Electronics",
      "items": [
        {"id": 101, "name": "Laptop", "price": 999.99},
        {"id": 102, "name": "Mouse", "price": 25.99}
      ]
    },
    {
      "category_id": 2,
      "category_name": "Books",
      "items": [
        {"id": 201, "name": "Java Guide", "price": 49.99},
        {"id": 202, "name": "Python Basics", "price": 39.99}
      ]
    }
  ]
}
```

**Flattened Table**:
```
response_id | response_timestamp      | category_id | category_name | item_id | item_name     | price
12345       | 2023-12-20T10:00:00Z   | 1           | Electronics   | 101     | Laptop        | 999.99
12345       | 2023-12-20T10:00:00Z   | 1           | Electronics   | 102     | Mouse         | 25.99
12345       | 2023-12-20T10:00:00Z   | 2           | Books         | 201     | Java Guide    | 49.99
12345       | 2023-12-20T10:00:00Z   | 2           | Books         | 202     | Python Basics | 39.99
```

Expected: 4 rows (root fields `response_id` and `response_timestamp` repeat for each item)

Same structures supported in XML format.

---

#### 7. Dynamic Request Headers - [RequestHeadersTest.java](src/test/java/org/apache/calcite/adapter/restapi/tests/RequestHeadersTest.java)

**What it tests**: Custom HTTP headers via FreeMarker templates.

**Configuration**:
```yaml
x-calcite-restapi-request-headers-template: classpath:templates/custom-headers-template.ftl
```

**FreeMarker Template**:
```
Authorization: Bearer ${jwtToken}
API-Version: ${restApiVersion}
Content-Type: ${contentType}
```

**Properties** (passed to adapter):
```properties
jwtToken=SOME_JWT_TOKEN_HERE
restApiVersion=v2.0
contentType=application/json
```

**Generated HTTP Headers**:
```
Authorization: Bearer SOME_JWT_TOKEN_HERE
API-Version: v2.0
Content-Type: application/json
```

Use case: Authentication tokens, API versioning, custom headers per request.

---

#### 8. CSV Response Parsing - [CsvResponseTest.java](src/test/java/org/apache/calcite/adapter/restapi/tests/CsvResponseTest.java)

**What it tests**: Parsing CSV format responses (alternative to JSON/XML).

**CSV Response** (Content-Type: text/csv):
```csv
id,name,lastName,age,department
1,Bob,Smith,30,IT
2,John,Doe,25,HR
3,Martin,Brown,35,IT
...
```

**SQL Query**:
```sql
SELECT * FROM users WHERE department = 'IT' ORDER BY id
```

**HTTP Request**:
```json
{
  "filters": [{"name": "department", "operator": "=", "value": "IT"}]
}
```

Expected: 4 IT users (ids 1,3,5,7)

**Comparison**: JSON vs CSV for same data
- CSV: Smaller size, simpler parsing
- JSON: More flexible, nested structures

---

#### 9. XML Request & Response - [XmlRequestResponseTest.java](src/test/java/org/apache/calcite/adapter/restapi/tests/XmlRequestResponseTest.java)

**What it tests**: XML request body generation and XML response parsing.

**XML Response** (Content-Type: application/xml):
```xml
<?xml version="1.0" encoding="UTF-8"?>
<books>
  <book>
    <bookId>1</bookId>
    <bookTitle>The Great Gatsby</bookTitle>
    <bookGenre>Fiction</bookGenre>
    <bookYear>1925</bookYear>
  </book>
  <book>
    <bookId>2</bookId>
    <bookTitle>1984</bookTitle>
    <bookGenre>Dystopian</bookGenre>
    <bookYear>1949</bookYear>
  </book>
</books>
```

**SQL Query**:
```sql
SELECT * FROM books WHERE book_genre = 'Fiction'
```

**XML Request Body** (generated from FreeMarker template):
```xml
<?xml version="1.0" encoding="UTF-8"?>
<request>
  <filters>
    <filter>
      <name>book_genre</name>
      <operator>=</operator>
      <value>Fiction</value>
    </filter>
  </filters>
  <page>1</page>
  <limit>100</limit>
</request>
```

Expected: 3 Fiction books (The Great Gatsby, To Kill a Mockingbird, The Catcher in the Rye)

---

#### 10. URL Query Parameters - [UrlFiltersTest.java](src/test/java/org/apache/calcite/adapter/restapi/tests/UrlFiltersTest.java)

**What it tests**: Filters in URL query parameters instead of request body.

**Configuration**:
```yaml
x-calcite-restapi-request-url-template: classpath:templates/url-filters-template.ftl
```

**FreeMarker Template**:
```
${baseUrl}/users?<#if filters?has_content><#list filters as filter>${filter.name}=${filter.value}&</#list></#if>limit=${limit}
```

**SQL Query**:
```sql
SELECT name, age FROM users WHERE name = 'Alice' LIMIT 10
```

**Generated URL**:
```
http://localhost:8080/users?name=Alice&limit=10
```

Use case: GET requests, REST APIs that use query parameters instead of request body.

---

#### 11. Type Mappings - [TypeMappingsTest.java](src/test/java/org/apache/calcite/adapter/restapi/tests/TypeMappingsTest.java)

**What it tests**: SQL type mappings via x-calcite-restapi-mappings.

**Configuration**:
```yaml
x-calcite-restapi-mappings:
  - name: products
    fields:
      - name: product_id
        type: INTEGER
      - name: product_name
        type: VARCHAR
      - name: price
        type: DECIMAL
      - name: in_stock
        type: BOOLEAN
      - name: created_at
        type: TIMESTAMP
```

**SQL Query**:
```sql
SELECT * FROM products ORDER BY product_id
```

**Type Verification** (via ResultSetMetaData):
```
product_id: INTEGER (java.lang.Integer)
product_name: VARCHAR (java.lang.String)
price: DECIMAL (java.math.BigDecimal)
in_stock: BOOLEAN (java.lang.Boolean)
created_at: TIMESTAMP (java.sql.Timestamp)
```

Ensures correct type conversion between REST API (JSON) and SQL.

---

#### 12. Nested XML Structures - [NestedStructureXmlTest.java](src/test/java/org/apache/calcite/adapter/restapi/tests/NestedStructureXmlTest.java)

**What it tests**: Same as NestedStructureTest but for XML format.

**Nested XML**:
```xml
<departments>
  <department>
    <id>1</id>
    <name>IT</name>
    <employees>
      <employee>
        <id>101</id>
        <name>Alice</name>
        <tasks>
          <task>
            <id>1001</id>
            <title>Fix bug</title>
          </task>
        </tasks>
      </employee>
    </employees>
  </department>
</departments>
```

Flattened the same way as JSON nested structures.

---

#### 13. REQUEST-only Parameters - [RequestOnlyParametersTest.java](src/test/java/org/apache/calcite/adapter/restapi/tests/RequestOnlyParametersTest.java)

**What it tests**: Handling of parameters that are used in REST API requests but are NOT returned in the response. Demonstrates parameter direction (`REQUEST`, `RESPONSE`, `BOTH`) and validation for unsupported operators.

**Real-world scenario**: Company lookup API where `rating` parameter (1 -10) is used to filter companies, but the parameter itself is not returned in the response. The response contains full company details (id, name, industry, revenue, employees) for each matching company.

**OpenAPI Parameter Directions**:
- `REQUEST` - Parameter used only in request (not in response)
- `RESPONSE` - Parameter only in response (not filterable)
- `BOTH` - Parameter in both request and response (filterable and returned)

**Example 1**: REQUEST-only parameter with `=` operator (supported)
```sql
SELECT rating, id, name, industry, revenue, employee_count
FROM company_schema.companies
WHERE rating = 10
```

HTTP Request:
```json
{
  "page": 1,
  "limit": 100,
  "rating": 10
}
```

HTTP Response (note: `rating` parameter NOT in response, but company data is returned):
```json
{
  "companies": [
    {"id": 1, "name": "TechCorp", "industry": "Technology", "revenue": 5000000.0, "employees": 50},
    {"id": 2, "name": "MegaRetail", "industry": "Retail", "revenue": 12000000.0, "employees": 120},
    {"id": 3, "name": "FinanceGroup", "industry": "Finance", "revenue": 3500000.0, "employees": 35}
  ]
}
```

SQL Result (ids substituted from WHERE clause):
```
rating | id | name          | industry    | revenue     | employee_count
10     | 1  | TechCorp      | Technology  | 5000000.0   | 50
10     | 2  | MegaRetail    | Retail      | 12000000.0  | 120
10     | 3  | FinanceGroup  | Finance     | 3500000.0   | 35
```

**Why it works**:
1. Filter pushdown sends `rating = '10` to REST API
2. REST API filters companies by rating
3. Response includes company details but NOT the rating parameter
4. Adapter substitutes rating value from WHERE clause into result rows

**Example 2**: REQUEST-only parameter with `>` operator (throws error)
```sql
SELECT rating, id, name, revenue, employee_count
FROM company_schema.companies
WHERE rating > '1'
```

**What happens**:
1. Adapter validation detects REQUEST-only parameter with non-`=` operator
2. Throws `AssertionError` with clear explanation
3. Query fails immediately with helpful error message

**Error message**:
```
Cannot execute query: Field 'rating' is a REQUEST-only parameter (not returned in REST API response).
Only '=' operator is supported for REQUEST-only fields.
Current query uses '>' operator which requires actual field values from response for each row.
Possible solutions:
  1) Change query to use '=' operator: WHERE rating = <value>
  2) Mark field 'rating' as RESPONSE or BOTH direction in OpenAPI specification
```

**Why `>` is not supported for REQUEST-only params**:
- With `=` operator: We know the exact value, can substitute it in results
- With `>`, `<`, `<>`: We don't know actual values for each row (they're not in response)
- Calcite needs actual values for filtering, sorting, grouping, joins
- **Throwing error is better than returning wrong/incomplete data**

**Validation rules**:
- REQUEST-only + `=` operator → ✅ Allowed (value substituted in results)
- REQUEST-only + `>`, `<`, `<>`, `>=`, `<=` → ❌ **Throws error immediately**
- RESPONSE or BOTH → ✅ All operators allowed (values present in response)

**OpenAPI specification example**:
```yaml
x-calcite-restapi-filterable-fields:
  - name: rating
    type: integer

requestBody:
  content:
    application/json:
      schema:
        properties:
          rating:
            type: integer
            description: rating 1-10

responses:
  '200':
    content:
      application/json:
        schema:
          properties:
            companies:
              type: array
              items:
                properties:
                  id:
                    type: integer
                  revenue:
                    type: number
                  employees:
                    type: integer
                # Note: company_name NOT in response schema
```

**Test coverage**:
1. ✅ REQUEST-only with `=` operator - works, value substituted
2. ✅ REQUEST-only with `>` operator - throws clear error
3. ✅ REQUEST-only with `<` operator - throws clear error
4. ✅ REQUEST-only with `<>` operator - throws clear error

---

### Test Infrastructure

**Base Classes**:
- [BaseTest.java](src/test/java/org/apache/calcite/adapter/restapi/tests/base/BaseTest.java) - WireMock setup, Calcite connection management, static response handling

**Testing Approach**:
- WireMock for HTTP mocking (no real HTTP calls)
- Static response files (JSON, XML, CSV) in `src/test/resources/rest/[TestClassName]/responses/`
- Request verification via `matchingJsonPath`, `equalTo`, `notMatching`
- Tests verify adapter behavior, not REST service behavior
- Comprehensive HTTP request inspection (body, headers, URL)

**Test Statistics**:
- 13 test classes
- 50+ test methods
- All tests passing ✅

All tests use static responses to ensure fast, reliable, and maintainable test execution without external dependencies.

***

## REST API Filter Specification Variants

Different REST APIs use different ways to represent filter logic.

Some REST APIs only allow simple filters: conditions combined with AND and limited to the equality operator (=) only.
In this case, all filter parameters are flat, and nested or OR conditions are not supported.

More advanced REST APIs may support filters written in Disjunctive Normal Form (DNF) — logical expressions that combine AND and OR operators in a structured way.

In DNF, a formula is a disjunction (OR) of one or more conjunctions (AND).

The request body structure can look like this:
- `where`: a list of conditions combined with AND (`filters[0]`)
- `or`: each entry is a group of ANDed conditions; groups are joined with OR (`filters[1..]`)

Some REST APIs instead use Conjunctive Normal Form (CNF) — logical expressions that also use AND and OR, but in reverse order.

In CNF, a formula is a conjunction (AND) of one or more disjunctions (OR).

The request body structure may look like this:
- `where`: a list of conditions combined with OR (`filters[0]`)
- `and`: each entry is a group of ORed conditions; groups are joined with AND (`filters[1..]`)

Below are the supported variants:

### Variant 1: Simple flat filters (no DNF/CNF support)

**Description:** REST API accepts only simple flat filters where all conditions are joined by AND with equality operator (=) only. Field names appear as direct properties in the request body, not nested structures.

**OpenAPI specification:**
```yaml
paths:
  /users:
    post:
      operationId: users
      x-calcite-restapi-table-name: users
      x-calcite-restapi-request-body-template: variant1-body-json-template.ftl
      # Explicitly define filterable parameters (BOTH direction - in request and response)
      parameters:
        - name: name
          in: query
          description: Filter by name (exact match only)
          schema:
            type: string
        - name: lastName
          in: query
          description: Filter by lastName (exact match only)
          schema:
            type: string
        - name: age
          in: query
          description: Filter by age (exact match only)
          schema:
            type: integer
        - name: department
          in: query
          description: Filter by department (exact match only)
          schema:
            type: string
      requestBody:
        required: true
        content:
          application/json:
            schema:
              type: object
              properties:
                page:
                  type: integer
                limit:
                  type: integer
                name:
                  type: string
                  description: Filter by name (exact match only)
                lastName:
                  type: string
                  description: Filter by lastName (exact match only)
                age:
                  type: integer
                  description: Filter by age (exact match only)
                department:
                  type: string
                  description: Filter by department (exact match only)
              additionalProperties: false
      responses:
        '200':
          description: User list
          content:
            application/json:
              schema:
                $ref: '#/components/schemas/UserList'
components:
  schemas:
    UserList:
      type: object
      properties:
        users:
          type: array
          items:
            $ref: '#/components/schemas/User'
    User:
      type: object
      x-calcite-restapi-mappings:
        id: id
        name: name
        lastName: last_name
        age: age
        department: department
      properties:
        id:
          type: integer
        name:
          type: string
        lastName:
          type: string
        age:
          type: integer
        department:
          type: string
```

**Key points:**
- **Uses POST operation** with JSON body for complex filter structures
- **`x-calcite-restapi-filterable-fields` extension** defines filterable fields
  - Explicit, clear declaration of which fields can be filtered
  - Semantically correct (not misleading like `parameters` would be for POST)
  - No confusion with actual query parameters
- Fields appear in both extension and response schema → Direction = **BOTH**
- `requestBody` schema defines the exact request structure
- All filter fields must also be in response schema for proper direction detection
- The adapter automatically processes filterable fields from the extension

**Freemarker template:**
```freemarker
{
  "page": ${(offset / limit)?int},
  "limit": ${limit}<#if filters_dnf?has_content && (filters_dnf?size > 0)>,
  <#-- Variant 1: Simple flat filters - only supports single AND group with = operator -->
  <#if (filters_dnf?size > 1)><#stop "Error: REST API does not support OR operators"></#if>
  <#list filters_dnf[0] as criterion>
  <#if (criterion.operator != '=')><#stop "Error: Only '=' operator is supported. Found operator: '${criterion.operator}'"></#if>
  "${criterion.name}":"${criterion.value}"<#if criterion?has_next>,</#if>
  </#list></#if>
}

```

**Example scenario:**

REST API contains the following users:
```json
[
  { "id": 1, "name": "Bob", "lastName": "Smith", "age": 25, "department": "IT" },
  { "id": 2, "name": "Alice", "lastName": "Johnson", "age": 22, "department": "HR" },
  { "id": 3, "name": "Bob", "lastName": "Brown", "age": 19, "department": "IT" },
  { "id": 4, "name": "Martin", "lastName": "Smith", "age": 35, "department": "Sales" },
  { "id": 5, "name": "Alice", "lastName": "Smith", "age": 28, "department": "IT" }
]
```

**SQL Query (simple AND conditions only):**
```sql
SELECT * FROM users
WHERE name = 'Alice' AND department = 'IT'
```

**Generated REST request body:**
```json
{
  "page": 0,
  "limit": 100,
  "name": "Alice",
  "department": "IT"
}
```

**Result:** Returns user with id: 5 (Alice Smith, 28, IT)

**Limitations:**

Queries with OR operators will be rejected or filtered locally on Calcite core side:
```sql
SELECT * FROM users
WHERE name = 'Alice' OR name = 'Bob'
```

Queries with non-equality operators will be rejected or filtered locally on Calcite core side:
```sql
SELECT * FROM users
WHERE age >= 25
```

**Use case:** This variant is suitable for simple REST APIs that:
- Only filter by exact field values
- Don't support complex logical expressions
- Have limited query capabilities
- Require minimal request payload

---

### Variant 2: DNF with "where" (AND) + "or" (OR groups)

**Description:** REST API accepts:
- `where`: array of filter criteria joined by AND (common conditions from first DNF group)
- `or`: array of arrays, where each inner array contains criteria joined by AND, and outer array represents OR

**OpenAPI specification:**
```yaml
requestBody:
  content:
    application/json:
      schema:
        properties:
          where:
            type: array
            items:
              $ref: '#/components/schemas/FilterCriterion'
            description: List of AND filter criteria
          or:
            type: array
            items:
              type: array
              items:
                $ref: '#/components/schemas/FilterCriterion'
            description: DNF - List of OR groups
```

**Freemarker template:**
```freemarker
{
  "page": ${(offset / limit)?int},
  "limit": ${limit}<#if filters_dnf?has_content && (filters_dnf?size > 0)>,
  <#-- Variant 2: DNF with 'where' (first AND group) + 'or' (remaining OR groups) -->
  "where": [
    <#list filters_dnf[0] as criterion>
    {
      "name": "${criterion.name}",
      "operator": "${criterion.operator}",
      "value": "${criterion.value}"
    }<#if criterion?has_next>,</#if>
    </#list>
  ]<#if (filters_dnf?size > 1)>,
  "or": [
    <#list filters_dnf[1..] as orGroup>
    [
      <#list orGroup as criterion>
      {
        "name": "${criterion.name}",
        "operator": "${criterion.operator}",
        "value": "${criterion.value}"
      }<#if criterion?has_next>,</#if>
      </#list>
    ]<#if orGroup?has_next>,</#if>
    </#list>
  ]</#if></#if>
}
```

**Example scenario:**

REST API contains the following users:
```json
[
  { "id": 1, "name": "Bob", "lastName": "Smith", "age": 25, "department": "IT" },
  { "id": 2, "name": "Alice", "lastName": "Johnson", "age": 22, "department": "HR" },
  { "id": 3, "name": "Bob", "lastName": "Brown", "age": 19, "department": "IT" },
  { "id": 4, "name": "Martin", "lastName": "Smith", "age": 35, "department": "Sales" },
  { "id": 5, "name": "Alice", "lastName": "Smith", "age": 28, "department": "IT" },
  { "id": 6, "name": "John", "lastName": "Doe", "age": 30, "department": "HR" },
  { "id": 7, "name": "Sarah", "lastName": "Smith", "age": 26, "department": "IT" }
]
```

**SQL Query with nested conditions:**
```sql
SELECT * FROM users
WHERE (
    (name = 'Bob' AND age >= 21)
    OR
    (name = 'Alice' AND (department = 'IT' OR age >= 25))
  )
  AND
  (last_name = 'Smith' OR (department = 'IT' AND age < 30))
```

**DNF Expansion:**
```
(name = 'Bob' AND age >= 21 AND last_name = 'Smith')
OR (name = 'Bob' AND age >= 21 AND department = 'IT' AND age < 30)
OR (name = 'Alice' AND department = 'IT' AND last_name = 'Smith')
OR (name = 'Alice' AND department = 'IT' AND department = 'IT' AND age < 30)
OR (name = 'Alice' AND age >= 25 AND last_name = 'Smith')
OR (name = 'Alice' AND age >= 25 AND department = 'IT' AND age < 30)
```

**Generated REST request body:**
```json
{
  "page": 0,
  "limit": 100,
  "where": [
    { "name": "name", "operator": "=", "value": "Bob" },
    { "name": "age", "operator": ">=", "value": 21 },
    { "name": "lastName", "operator": "=", "value": "Smith" }
  ],
  "or": [
    [
      { "name": "name", "operator": "=", "value": "Bob" },
      { "name": "age", "operator": ">=", "value": 21 },
      { "name": "department", "operator": "=", "value": "IT" },
      { "name": "age", "operator": "<", "value": 30 }
    ],
    [
      { "name": "name", "operator": "=", "value": "Alice" },
      { "name": "department", "operator": "=", "value": "IT" },
      { "name": "lastName", "operator": "=", "value": "Smith" }
    ],
    [
      { "name": "name", "operator": "=", "value": "Alice" },
      { "name": "department", "operator": "=", "value": "IT" },
      { "name": "age", "operator": "<", "value": 30 }
    ],
    [
      { "name": "name", "operator": "=", "value": "Alice" },
      { "name": "age", "operator": ">=", "value": 25 },
      { "name": "lastName", "operator": "=", "value": "Smith" }
    ],
    [
      { "name": "name", "operator": "=", "value": "Alice" },
      { "name": "age", "operator": ">=", "value": 25 },
      { "name": "department", "operator": "=", "value": "IT" },
      { "name": "age", "operator": "<", "value": 30 }
    ]
  ]
}
```

**Result:** Returns users with id: 1 (Bob Smith, 25, IT) and 5 (Alice Smith, 28, IT)

---

### Variant 3: CNF with "where" (OR) + "and" (AND groups)

**Description:** REST API accepts:
- `where`: array of filter criteria joined by OR (conditions from first CNF group)
- `and`: array of arrays, where each inner array contains criteria joined by OR, and outer array represents AND

**OpenAPI specification:**
```yaml
requestBody:
  content:
    application/json:
      schema:
        properties:
          where:
            type: array
            items:
              $ref: '#/components/schemas/FilterCriterion'
            description: List of OR filter criteria
          and:
            type: array
            items:
              type: array
              items:
                $ref: '#/components/schemas/FilterCriterion'
            description: CNF - List of AND groups (each group contains OR criteria)
```

```freemarker
{
  "page": ${(offset / limit)?int},
  "limit": ${limit}<#if filters_cnf?has_content && (filters_cnf?size > 0)>,
  <#-- Variant 4: CNF with 'where' (first OR group) + 'and' (remaining AND groups) -->
  "where": [
    <#list filters_cnf[0] as criterion>
    {
      "name": "${criterion.name}",
      "operator": "${criterion.operator}",
      "value": "${criterion.value}"
    }<#if criterion?has_next>,</#if>
    </#list>
  ]<#if (filters_cnf?size > 1)>,
  "and": [
    <#list filters_cnf[1..] as andGroup>
    [
      <#list andGroup as criterion>
      {
        "name": "${criterion.name}",
        "operator": "${criterion.operator}",
        "value": "${criterion.value}"
      }<#if criterion?has_next>,</#if>
      </#list>
    ]<#if andGroup?has_next>,</#if>
    </#list>
  ]</#if></#if>
}
```

**Example scenario:**

REST API contains the following users:
```json
[
  { "id": 1, "name": "Bob", "lastName": "Smith", "age": 25, "department": "IT" },
  { "id": 2, "name": "Alice", "lastName": "Johnson", "age": 22, "department": "HR" },
  { "id": 3, "name": "Bob", "lastName": "Brown", "age": 19, "department": "IT" },
  { "id": 4, "name": "Martin", "lastName": "Smith", "age": 35, "department": "Sales" },
  { "id": 5, "name": "Alice", "lastName": "Smith", "age": 28, "department": "IT" },
  { "id": 6, "name": "John", "lastName": "Doe", "age": 30, "department": "HR" },
  { "id": 7, "name": "Sarah", "lastName": "Smith", "age": 26, "department": "IT" }
]
```

**SQL Query with nested conditions:**
```sql
SELECT * FROM users
WHERE (
    (name = 'Bob' AND age >= 21)
    OR
    (name = 'Alice' AND (department = 'IT' OR age >= 25))
  )
  AND
  (last_name = 'Smith' OR (department = 'IT' AND age < 30))
```

**CNF Expansion:**
```
(name = 'Bob' OR name = 'Alice')
AND (name = 'Bob' OR name = 'Alice' OR age >= 25)
AND (name = 'Bob' OR name = 'Alice' OR department = 'IT')
AND (name = 'Bob' OR department = 'IT' OR age >= 25)
AND (name = 'Alice' OR age >= 21)
AND (name = 'Alice' OR age >= 21 OR department = 'IT')
AND (age >= 21 OR department = 'IT' OR age >= 25)
AND (last_name = 'Smith' OR department = 'IT')
AND (last_name = 'Smith' OR age < 30)
```

**Generated REST request body:**
```json
{
  "page": 0,
  "limit": 100,
  "where": [
    { "name": "name", "operator": "=", "value": "Bob" },
    { "name": "name", "operator": "=", "value": "Alice" }
  ],
  "and": [
    [
      { "name": "name", "operator": "=", "value": "Bob" },
      { "name": "name", "operator": "=", "value": "Alice" },
      { "name": "age", "operator": ">=", "value": 25 }
    ],
    [
      { "name": "name", "operator": "=", "value": "Bob" },
      { "name": "name", "operator": "=", "value": "Alice" },
      { "name": "department", "operator": "=", "value": "IT" }
    ],
    [
      { "name": "name", "operator": "=", "value": "Bob" },
      { "name": "department", "operator": "=", "value": "IT" },
      { "name": "age", "operator": ">=", "value": 25 }
    ],
    [
      { "name": "name", "operator": "=", "value": "Alice" },
      { "name": "age", "operator": ">=", "value": 21 }
    ],
    [
      { "name": "name", "operator": "=", "value": "Alice" },
      { "name": "age", "operator": ">=", "value": 21 },
      { "name": "department", "operator": "=", "value": "IT" }
    ],
    [
      { "name": "age", "operator": ">=", "value": 21 },
      { "name": "department", "operator": "=", "value": "IT" },
      { "name": "age", "operator": ">=", "value": 25 }
    ],
    [
      { "name": "lastName", "operator": "=", "value": "Smith" },
      { "name": "department", "operator": "=", "value": "IT" }
    ],
    [
      { "name": "lastName", "operator": "=", "value": "Smith" },
      { "name": "age", "operator": "<", "value": 30 }
    ]
  ]
}
```

**Result:** Returns users with id: 1 (Bob Smith, 25, IT) and 5 (Alice Smith, 28, IT)

---

### Variant 4: DNF with "filters" only (ambiguous field)

**Description:** REST API uses generic field name like `filters` in DNF form.

**OpenAPI specification:**
```yaml
requestBody:
  content:
    application/json:
      schema:
        properties:
          filters:
            type: array
            items:
              type: array
              items:
                $ref: '#/components/schemas/FilterCriterion'
            description: "DNF: Array of OR groups, each group contains ANDed criteria"
```

```freemarker
{
  "page": ${(offset / limit)?int},
  "limit": ${limit}<#if filters_dnf?has_content && (filters_dnf?size > 0)>,
  <#-- Variant 6: DNF with 'filters' field only (ambiguous field name) -->
  "filters": [
    <#list filters_dnf as orGroup>
    [
      <#list orGroup as criterion>
      {
        "name": "${criterion.name}",
        "operator": "${criterion.operator}",
        "value": "${criterion.value}"
      }<#if criterion?has_next>,</#if>
      </#list>
    ]<#if orGroup?has_next>,</#if>
    </#list>
  ]</#if>
}
```


**Example scenario:**

REST API contains the following users:
```json
[
  { "id": 1, "name": "Bob", "lastName": "Smith", "age": 25, "department": "IT" },
  { "id": 2, "name": "Alice", "lastName": "Johnson", "age": 22, "department": "HR" },
  { "id": 3, "name": "Bob", "lastName": "Brown", "age": 19, "department": "IT" },
  { "id": 4, "name": "Martin", "lastName": "Smith", "age": 35, "department": "Sales" },
  { "id": 5, "name": "Alice", "lastName": "Smith", "age": 28, "department": "IT" },
  { "id": 6, "name": "John", "lastName": "Doe", "age": 30, "department": "HR" },
  { "id": 7, "name": "Sarah", "lastName": "Smith", "age": 26, "department": "IT" }
]
```

**SQL Query with nested conditions:**
```sql
SELECT * FROM users
WHERE (
    (name = 'Bob' AND age >= 21)
    OR
    (name = 'Alice' AND (department = 'IT' OR age >= 25))
  )
  AND
  (last_name = 'Smith' OR (department = 'IT' AND age < 30))
```

**DNF Expansion:**
```
(name = 'Bob' AND age >= 21 AND last_name = 'Smith')
OR (name = 'Bob' AND age >= 21 AND department = 'IT' AND age < 30)
OR (name = 'Alice' AND department = 'IT' AND last_name = 'Smith')
OR (name = 'Alice' AND department = 'IT' AND age < 30)
OR (name = 'Alice' AND age >= 25 AND lastName = 'Smith')
OR (name = 'Alice' AND age >= 25 AND department = 'IT' AND age < 30)
```

**Generated REST request body:**
```json
{
  "page": 0,
  "limit": 100,
  "filters": [
    [
      { "name": "name", "operator": "=", "value": "Bob" },
      { "name": "age", "operator": ">=", "value": 21 },
      { "name": "lastName", "operator": "=", "value": "Smith" }
    ],
    [
      { "name": "name", "operator": "=", "value": "Bob" },
      { "name": "age", "operator": ">=", "value": 21 },
      { "name": "department", "operator": "=", "value": "IT" },
      { "name": "age", "operator": "<", "value": 30 }
    ],
    [
      { "name": "name", "operator": "=", "value": "Alice" },
      { "name": "department", "operator": "=", "value": "IT" },
      { "name": "lastName", "operator": "=", "value": "Smith" }
    ],
    [
      { "name": "name", "operator": "=", "value": "Alice" },
      { "name": "department", "operator": "=", "value": "IT" },
      { "name": "age", "operator": "<", "value": 30 }
    ],
    [
      { "name": "name", "operator": "=", "value": "Alice" },
      { "name": "age", "operator": ">=", "value": 25 },
      { "name": "lastName", "operator": "=", "value": "Smith" }
    ],
    [
      { "name": "name", "operator": "=", "value": "Alice" },
      { "name": "age", "operator": ">=", "value": 25 },
      { "name": "department", "operator": "=", "value": "IT" },
      { "name": "age", "operator": "<", "value": 30 }
    ]
  ]
}
```

**Result:** Returns users with id: 1 (Bob Smith, 25, IT) and 5 (Alice Smith, 28, IT)

---

### Variant 5: CNF with "filters" only

**Description:** REST API uses generic field name like `filters` in CNF form.

**OpenAPI specification:**
```yaml
requestBody:
  content:
    application/json:
      schema:
        properties:
          filters:
            type: array
            items:
              type: array
              items:
                $ref: '#/components/schemas/FilterCriterion'
            description: "CNF: Array of AND groups, each group contains ORed criteria"
```

```freemarker
{
  "page": ${(offset / limit)?int},
  "limit": ${limit}<#if filters_cnf?has_content && (filters_cnf?size > 0)>,
  <#-- Variant 7: CNF with 'filters' field only (ambiguous field name) -->
  "filters": [
    <#list filters_cnf as andGroup>
    [
      <#list andGroup as criterion>
      {
        "name": "${criterion.name}",
        "operator": "${criterion.operator}",
        "value": "${criterion.value}"
      }<#if criterion?has_next>,</#if>
      </#list>
    ]<#if andGroup?has_next>,</#if>
    </#list>
  ]</#if>
}
```

**Example scenario:**

REST API contains the following users:
```json
[
  { "id": 1, "name": "Bob", "lastName": "Smith", "age": 25, "department": "IT" },
  { "id": 2, "name": "Alice", "lastName": "Johnson", "age": 22, "department": "HR" },
  { "id": 3, "name": "Bob", "lastName": "Brown", "age": 19, "department": "IT" },
  { "id": 4, "name": "Martin", "lastName": "Smith", "age": 35, "department": "Sales" },
  { "id": 5, "name": "Alice", "lastName": "Smith", "age": 28, "department": "IT" },
  { "id": 6, "name": "John", "lastName": "Doe", "age": 30, "department": "HR" },
  { "id": 7, "name": "Sarah", "lastName": "Smith", "age": 26, "department": "IT" }
]
```

**SQL Query with nested conditions:**
```sql
SELECT * FROM users
WHERE (
    (name = 'Bob' AND age >= 21)
    OR
    (name = 'Alice' AND (department = 'IT' OR age >= 25))
  )
  AND
  (last_name = 'Smith' OR (department = 'IT' AND age < 30))
```

**CNF Expansion:**
```
(name = 'Bob' OR name = 'Alice')
AND (name = 'Bob' OR name = 'Alice' OR age >= 25)
AND (name = 'Bob' OR name = 'Alice' OR department = 'IT')
AND (name = 'Bob' OR department = 'IT' OR age >= 25)
AND (name = 'Alice' OR age >= 21)
AND (name = 'Alice' OR age >= 21 OR department = 'IT')
AND (age >= 21 OR department = 'IT' OR age >= 25)
AND (last_name = 'Smith' OR department = 'IT')
AND (last_name = 'Smith' OR age < 30)
```

**Generated REST request body:**
```json
{
  "page": 0,
  "limit": 100,
  "filters": [
    [
      { "name": "name", "operator": "=", "value": "Bob" },
      { "name": "name", "operator": "=", "value": "Alice" }
    ],
    [
      { "name": "name", "operator": "=", "value": "Bob" },
      { "name": "name", "operator": "=", "value": "Alice" },
      { "name": "age", "operator": ">=", "value": 25 }
    ],
    [
      { "name": "name", "operator": "=", "value": "Bob" },
      { "name": "name", "operator": "=", "value": "Alice" },
      { "name": "department", "operator": "=", "value": "IT" }
    ],
    [
      { "name": "name", "operator": "=", "value": "Bob" },
      { "name": "department", "operator": "=", "value": "IT" },
      { "name": "age", "operator": ">=", "value": 25 }
    ],
    [
      { "name": "name", "operator": "=", "value": "Alice" },
      { "name": "age", "operator": ">=", "value": 21 }
    ],
    [
      { "name": "name", "operator": "=", "value": "Alice" },
      { "name": "age", "operator": ">=", "value": 21 },
      { "name": "department", "operator": "=", "value": "IT" }
    ],
    [
      { "name": "age", "operator": ">=", "value": 21 },
      { "name": "department", "operator": "=", "value": "IT" },
      { "name": "age", "operator": ">=", "value": 25 }
    ],
    [
      { "name": "lastName", "operator": "=", "value": "Smith" },
      { "name": "department", "operator": "=", "value": "IT" }
    ],
    [
      { "name": "lastName", "operator": "=", "value": "Smith" },
      { "name": "age", "operator": "<", "value": 30 }
    ]
  ]
}
```

**Result:** Returns users with id: 1 (Bob Smith, 25, IT) and 5 (Alice Smith, 28, IT)

**Note:** If description doesn't contain "DNF" or "CNF", the adapter will throw an error asking user to provide a FreeMarker template.

**Test Coverage:** All 5 filter variants are tested in [AllVariantsIntegrationJsonTest.java](src/test/java/org/apache/calcite/adapter/restapi/tests/AllVariantsIntegrationJsonTest.java)

---

## Response Formats

The adapter supports multiple data formats for both REST API requests and responses:

### JSON Format

JSON is the most commonly used format. The adapter can parse:
- Simple flat JSON objects
- Nested JSON structures (arrays within objects, objects within arrays)
- Complex multi-level hierarchies

**Example:**
```json
{
  "users": [
    { "id": 1, "name": "Alice", "age": 25, "department": "IT" },
    { "id": 2, "name": "Bob", "age": 30, "department": "HR" }
  ]
}
```

**Test Coverage:** JSON response parsing is tested in multiple test classes including [NestedStructureJsonResponseTest.java](src/test/java/org/apache/calcite/adapter/restapi/tests/NestedStructureJsonResponseTest.java)

### XML Format

The adapter supports XML format for both request bodies and responses. XML parsing handles:
- XML elements mapped to SQL columns
- Nested XML structures
- XML attributes
- FreeMarker templates for XML request generation

**Example:**
```xml
<books>
  <book>
    <id>1</id>
    <title>The Great Gatsby</title>
    <author>F. Scott Fitzgerald</author>
    <year>1925</year>
  </book>
</books>
```

**Test Coverage:** XML request and response handling is tested in [XmlRequestResponseTest.java](src/test/java/org/apache/calcite/adapter/restapi/tests/XmlRequestResponseTest.java)

**Example: Flattening and Querying Nested XML**

The adapter can flatten nested XML data into a single relational table, similar to how it handles JSON. This is enabled by defining response content as `application/xml` and using the `x-calcite-restapi-mappings` extension.

The complete working example can be found in the [NestedStructureXmlTest.java](src/test/java/org/apache/calcite/adapter/restapi/tests/NestedStructureXmlTest.java) test.

**1. The Goal**

Our goal is to query a REST API that returns a multi-level XML structure of departments, employees, and tasks, and view it as a single, flat table.

**2. The Nested XML Response**

The API returns the following XML data, where `employees` are nested in `departments` and `tasks` are nested in `employees`.

```xml
<!-- src/test/resources/rest/NestedStructureXmlResponseTest/responses/departments-all.xml -->
<?xml version="1.0" encoding="UTF-8"?>
<departments>
  <department>
    <department_id>1</department_id>
    <department_name>IT</department_name>
    <employees>
      <employee>
        <employee_id>101</employee_id>
        <employee_name>Alice</employee_name>
        <tasks>
          <task>
            <task_id>1001</task_id>
            <task_title>Fix bug</task_title>
          </task>
        </tasks>
      </employee>
    </employees>
  </department>
</departments>
```

**3. OpenAPI Specification with Mappings**

In the OpenAPI definition, we specify that the response content type is `application/xml`. Crucially, we use `x-calcite-restapi-mappings` at each level of the hierarchy to define the SQL column names. The adapter uses these mappings to identify which fields to extract from the XML structure.

```yaml
# src/test/resources/rest/NestedStructureXmlResponseTest/neested-xml-response.yaml
paths:
  /departments:
    post:
      summary: Get departments with nested employees and tasks (XML format)
      responses:
        '200':
          description: Department list with nested employees and tasks in XML
          content:
            application/xml:  # Specify XML content type
              schema:
                $ref: '#/components/schemas/DepartmentList'
components:
  schemas:
    Department:
      type: object
      x-calcite-restapi-mappings:
        id: department_id
        name: department_name
      properties:
        #...
        employees:
          type: array
          items:
            $ref: '#/components/schemas/Employee'
    Employee:
      type: object
      x-calcite-restapi-mappings:
        id: employee_id
        name: employee_name
      properties:
        #...
        tasks:
          type: array
          items:
            $ref: '#/components/schemas/Task'
    Task:
      type: object
      x-calcite-restapi-mappings:
        id: task_id
        title: task_title
      properties: #...
```

**4. SQL Query and Flattened Result**

Now, we can execute a simple `SELECT *` query. The adapter automatically traverses the nested XML, picks out the mapped fields, and produces a flattened relational row for each unique path down the hierarchy.

```sql
SELECT department_name, employee_name, task_title
FROM departments_schema.departments
```

**Result Set:**

| department_name | employee_name | task_title |
|-----------------|---------------|------------|
| IT              | Alice         | Fix bug    |

This demonstrates how the adapter's mapping and flattening capabilities work seamlessly with nested XML, making complex hierarchical data easy to query with standard SQL.

### CSV Format

The adapter can parse CSV (Comma-Separated Values) responses. This is useful for:
- Simple tabular data
- Legacy systems that output CSV
- Data export APIs

**Example:**
```csv
id,name,age,department
1,Alice,25,IT
2,Bob,30,HR
```

**Test Coverage:** CSV response parsing is tested in [CsvResponseTest.java](src/test/java/org/apache/calcite/adapter/restapi/tests/CsvResponseTest.java)

### Format Selection

The response format is determined by:
1. The `Content-Type` header in the REST API response
2. The OpenAPI specification's response content type
3. Template configuration for request headers

All formats support the same SQL query capabilities including filtering, projections, pagination, and JOINs.

---


### Custom OpenAPI Extensions

The adapter supports custom OpenAPI extensions to provide control over schema interpretation. All extensions use the `x-calcite-restapi-` prefix to avoid conflicts with other tools.

#### Available Extensions

##### 1. `x-calcite-restapi-schema-name`
**Location:** `info` section (global schema name) or operation level (per-endpoint schema name)
**Type:** string
**Description:** Specifies a custom schema name. This extension can be used at two levels:
- **Info section**: Applies to the entire OpenAPI specification file, grouping all endpoints under one schema
- **Operation level**: Applies to individual endpoints, allowing multiple schemas to be defined within a single OpenAPI file

**Global schema name example (all endpoints in same schema):**
```yaml
openapi: 3.0.0
info:
  title: User Management API
  version: "1.0"
  x-calcite-restapi-schema-name: users
```

**Per-endpoint schema names example (multiple schemas in one file):**
```yaml
openapi: 3.0.0
info:
  title: Multi-Service API
  version: "1.0"
servers:
  - url: http://localhost:8080
paths:
  /users:
    post:
      operationId: users
      x-calcite-restapi-table-name: users
      x-calcite-restapi-schema-name: users_schema  # Schema name for this endpoint
      summary: Get users
  /departments:
    post:
      operationId: departments
      x-calcite-restapi-table-name: departments
      x-calcite-restapi-schema-name: departments_schema  # Schema name for this endpoint
      summary: Get departments
```

When both global and operation-level schema names are present, the operation-level extension takes precedence for that specific endpoint.

##### 2. `x-calcite-restapi-table-name`
**Location:** operation level (per endpoint method)
**Type:** string
**Description:** Overrides the default table name (which is derived from `operationId`).

**Example:**
```yaml
paths:
  /users:
    post:
      operationId: getUserList
      x-calcite-restapi-table-name: users
      summary: Get users with filters
```

##### 3. `x-calcite-restapi-pagination`
**Location:** operation level (per endpoint method)
**Type:** object
**Description:** Determines whether pagination is enabled and its parameters for the endpoint.

**Properties:**
- `pageStart` (integer): Starting page number (default: 1)
- `pageSize` (integer): Number of records per page

**Example:**
```yaml
paths:
  /users:
    post:
      operationId: users
      x-calcite-restapi-pagination:
        pageStart: 1
        pageSize: 100
```

##### 4. `x-calcite-restapi-request-body-template`
**Location:** operation level (per endpoint method)
**Type:** string
**Description:** Specifies the FreeMarker template file name (.ftl) used to generate the request body for this endpoint. The template file must be placed in the same directory as the OpenAPI specification file.

**Key Features:**
- Provides explicit control over request body formation
- Supports both DNF and CNF filter structures through `filters_dnf` and `filters_cnf` variables
- Replaces automatic filter detection with explicit template-based approach
- Templates can access all standard FreeMarker variables: `offset`, `limit`, `name`, `filters_dnf`, `filters_cnf`, `projects`, etc.

**Example:**
```yaml
paths:
  /users:
    post:
      operationId: users
      x-calcite-restapi-request-body-template: users-dnf-template.ftl
      summary: Get users with DNF filters
```

**Template file example** (users-dnf-template.ftl):
```freemarker
{
  "page": ${(offset / limit)?int},
  "limit": ${limit}<#if filters_dnf?has_content && (filters_dnf?size > 0)>,
  "where": [
    <#list filters_dnf[0] as criterion>
    {
      "name": "${criterion.name}",
      "operator": "${criterion.operator}",
      "value": "${criterion.value}"
    }<#if criterion?has_next>,</#if>
    </#list>
  ]<#if (filters_dnf?size > 1)>,
  "or": [
    <#list filters_dnf[1..] as orGroup>
    [
      <#list orGroup as criterion>
      {
        "name": "${criterion.name}",
        "operator": "${criterion.operator}",
        "value": "${criterion.value}"
      }<#if criterion?has_next>,</#if>
      </#list>
    ]<#if orGroup?has_next>,</#if>
    </#list>
  ]</#if></#if>
}
```

**Available template variables:**
- `offset` - Pagination offset
- `limit` - Page size
- `pageStart` - Starting page number
- `name` - Table name
- `filters_dnf` - List of filter groups in DNF form (OR-of-ANDs): `List<List<Map<String, String>>>` where outer list = OR groups, inner list = AND conditions
- `filters_cnf` - List of filter groups in CNF form (AND-of-ORs): `List<List<Map<String, String>>>` where outer list = AND groups, inner list = OR conditions
- `projects` - List of selected fields (from SELECT clause)
- Any custom variables passed via JDBC connection URL (e.g., `restApiVersion`, `contentType`, `jwtToken`)

**Note:** If this extension is not specified, the adapter will look for a template file named `{tableName}.ftl` in the same directory (backward compatibility).

##### 5. `x-calcite-restapi-request-url-template`
**Location:** operation level (per endpoint method)
**Type:** string
**Description:** Specifies the FreeMarker template file name (.ftl) used to generate the request URL for this endpoint. The template file must be placed in the same directory as the OpenAPI specification file. This provides full control over how SQL query elements (filters, projections, pagination) are mapped to the REST API's URL structure.

**Example: Passing Filters and Projections in the URL**

This example demonstrates how to pass SQL `WHERE` clauses and `SELECT` columns as query parameters in the request URL. This is a common pattern in many REST APIs.

The complete working example can be found in the [UrlFiltersTest.java](src/test/java/org/apache/calcite/adapter/restapi/tests/UrlFiltersTest.java) test.

**1. The Goal**

We want to execute the following SQL query and have the `WHERE` clause and selected fields (`name`, `age`) translated into URL query parameters.

```sql
SELECT name, age FROM users_schema.users WHERE name = 'Alice'
```

**2. OpenAPI Specification**

First, we specify the `x-calcite-restapi-request-url-template` in our OpenAPI definition (`url-filters.yaml`) to delegate URL construction to a template.

```yaml
# src/test/resources/rest/UrlFiltersTest/url-filters.yaml
paths:
  /users:
    post:
      operationId: users
      summary: Get users with URL template for filters, projections and pagination
      # Point to the FreeMarker template for URL generation
      x-calcite-restapi-request-url-template: url-template.ftl
      ...
```

**3. FreeMarker URL Template**

Next, we create the `url-template.ftl` file. This template accesses the built-in `projects` and `filters_dnf` variables to construct the query string.

```freemarker
<#-- src/test/resources/rest/UrlFiltersTest/url-template.ftl -->
<#setting url_escaping_charset='UTF-8'>

/users?<#-- Base path -->
<#-- Pagination parameters -->
<#if offset gte 0>offset=${offset}</#if><#if limit gt 0>&limit=${limit}</#if><#--
--><#-- Projection parameters (from SELECT) -->
<#if projects?has_content><#list projects as proj>&proj=${proj?url}</#list></#if><#--
--><#-- Filter parameters (from WHERE) -->
<#if filters_dnf?has_content && (filters_dnf?size > 0)>
    <#list filters_dnf as andGroup>
        <#list andGroup as criterion>&${criterion.name?url}=${criterion.value?string?url}</#list>
    </#list>
</#if>
```
*Note: This template is simplified for clarity. The actual test template includes basic error handling.*

**4. Generated HTTP Request**

When the SQL query is executed, the adapter uses the template to generate a `POST` request to the following URL path:

`/users?offset=0&limit=100&proj=name&proj=age&name=Alice`

- `proj=name` and `proj=age` are generated from the `SELECT name, age` clause.
- `name=Alice` is generated from the `WHERE name = 'Alice'` clause.
- `offset` and `limit` are included for pagination.

This level of control is essential for integrating with APIs that expect filters and field selections in the URL.

##### 6. `x-calcite-restapi-request-headers-template`
**Location:** operation level (per endpoint method)
**Type:** string
**Description:** Specifies the FreeMarker template file name (.ftl) used to generate the request headers for this endpoint. The template file must be placed in the same directory as the OpenAPI specification file.

**Key Features:**
- Provides explicit control over HTTP headers formation
- Supports dynamic header values using FreeMarker variables
- Can generate a JSON object representing header key-value pairs
- Templates can access all standard FreeMarker variables: `offset`, `limit`, `name`, `filters_dnf`, `filters_cnf`, `projects`, etc., as well as custom properties passed via JDBC connection URL

**Example:**
```yaml
paths:
  /users:
    post:
      operationId: users
      x-calcite-restapi-request-headers-template: users-headers-template.ftl
      summary: Get users with dynamic headers
```

**Template file example** (users-headers-template.ftl):
```freemarker
{
  "Content-Type": "${contentType}",
  "Authorization": "Bearer ${jwtToken}",
  "API-Version": "${restApiVersion}"
}
```

**Available template variables:**
- `offset` - Pagination offset
- `limit` - Page size
- `pageStart` - Starting page number
- `name` - Table name
- `filters_dnf` - List of filter groups in DNF form
- `filters_cnf` - List of filter groups in CNF form
- `projects` - List of selected fields
- Any custom variables passed via JDBC connection URL (e.g., `restApiVersion`, `contentType`, `jwtToken`)

**Note:** The template should output a JSON object with header key-value pairs. If using a different format, the template output will be parsed according to the implementation's parsing logic. Currently, the implementation supports JSON objects and a simple key:value format separated by newlines.

**Test Coverage:** Header template functionality is tested in [RequestHeadersTest.java](src/test/java/org/apache/calcite/adapter/restapi/tests/RequestHeadersTest.java)

##### Summary: Template Extensions
The adapter supports three complementary template extensions for complete REST request customization:
- `x-calcite-restapi-request-body-template`: Controls request body content
- `x-calcite-restapi-request-url-template`: Controls request URL and query parameters
- `x-calcite-restapi-request-headers-template`: Controls request headers

These extensions can be used individually or in combination to fully customize REST API requests. When multiple template extensions are specified, they work together to build the complete HTTP request.

All template extensions share the same set of available variables and support custom properties passed via JDBC connection URL.

##### 7. `x-calcite-restapi-mappings`
**Location:** schema level (within component schemas)
**Type:** object (map)
**Description:** Defines mappings between REST API field names and SQL column names. This extension is particularly useful for:
- Nested structures where multiple levels have fields with the same name (e.g., `id` in departments, employees, and tasks)
- APIs where REST field names don't match desired SQL column names (e.g., camelCase REST → snake_case SQL)
- Explicit SQL type mapping for fields that need specific SQL types

**Format:**
```yaml
x-calcite-restapi-mappings:
  restFieldName: sql_column_name
```

**Extended format with explicit SQL types:**
```yaml
x-calcite-restapi-mappings:
  restFieldName:
    sqlName: sql_column_name
    sqlType: VARCHAR  # Optional: explicit SQL type (e.g., DECIMAL, TIMESTAMP, etc.)
```

**Key Features:**
- Only properties with mappings defined will be included in the resulting SQL table
- Automatically handles filter field mapping (SQL→REST conversion for WHERE clauses)
- Supports explicit SQL type specification for precise type control
- Works recursively for nested structures

**Example - Simple mappings:**
```yaml
components:
  schemas:
    Department:
      type: object
      x-calcite-restapi-mappings:
        id: department_id
        name: department_name
        location: department_location
        budget: department_budget
      properties:
        id:
          type: integer
        name:
          type: string
        location:
          type: string
        budget:
          type: integer
        employees:
          type: array
          items:
            $ref: '#/components/schemas/Employee'
    Employee:
      type: object
      x-calcite-restapi-mappings:
        id: employee_id
        name: employee_name
        position: employee_position
        salary: employee_salary
      properties:
        id:
          type: integer
        name:
          type: string
        position:
          type: string
        salary:
          type: integer
        tasks:
          type: array
          items:
            $ref: '#/components/schemas/Task'
    Task:
      type: object
      x-calcite-restapi-mappings:
        id: task_id
        title: task_title
        status: task_status
        priority: task_priority
      properties:
        id:
          type: integer
        title:
          type: string
        status:
          type: string
        priority:
          type: integer
```

**Example - With explicit SQL types:**
```yaml
Product:
  type: object
  x-calcite-restapi-mappings:
    id: product_id
    price:
      sqlName: product_price
      sqlType: string  # Use string for decimal values in JSON
    quantity:
      sqlName: product_quantity
      sqlType: int     # Maps to INTEGER in SQL
    created_at:
      sqlName: created_timestamp
      sqlType: timestamp  # Maps to TIMESTAMP in SQL
    is_active:
      sqlName: is_active
      sqlType: boolean    # Maps to BOOLEAN in SQL
    weight:
      sqlName: product_weight
      sqlType: float      # Maps to FLOAT/REAL in SQL
    category_id:
      sqlName: category_id
      sqlType: long       # Maps to BIGINT in SQL
  properties:
    id:
      type: string
    price:
      type: string  # JSON string, but treated as decimal via mapping
    quantity:
      type: integer
    created_at:
      type: string
      format: date-time
    is_active:
      type: boolean
    weight:
      type: number
    category_id:
      type: integer
```

**Supported SQL Types:**
The following types are supported for the `sqlType` field:
- `string` - Maps to VARCHAR in SQL
- `boolean` - Maps to BOOLEAN in SQL
- `byte` - Maps to TINYINT in SQL
- `char` - Maps to CHAR in SQL
- `short` - Maps to SMALLINT in SQL
- `int` - Maps to INTEGER in SQL
- `long` - Maps to BIGINT in SQL
- `float` - Maps to FLOAT/REAL in SQL
- `double` - Maps to DOUBLE in SQL
- `date` - Maps to DATE in SQL
- `time` - Maps to TIME in SQL
- `timestamp` - Maps to TIMESTAMP in SQL
- `uuid` - Maps to UUID in SQL

**Example: Explicit SQL Type Mappings**

This example demonstrates how to explicitly map REST API fields to specific SQL data types using `x-calcite-restapi-mappings`, ensuring data integrity and correct type handling within Calcite.

The complete working example can be found in the [TypeMappingsTest.java](src/test/java/org/apache/calcite/adapter/restapi/tests/TypeMappingsTest.java) test.

**1. The Goal**

We aim to fetch product data where some REST API fields (like `price` as a string, `category_id` as an integer) need to be interpreted as specific SQL types (like `DOUBLE` and `BIGINT`, respectively) in our SQL schema.

**2. OpenAPI Specification (`type-mappings.yaml`)**

The OpenAPI definition for the `/products` endpoint includes a `Product` schema. Within this schema, the `x-calcite-restapi-mappings` extension is used to define not only the SQL column names but also their explicit `sqlType`.

```yaml
# src/test/resources/rest/TypeMappingsTest/type-mappings.yaml
components:
  schemas:
    Product:
      type: object
      x-calcite-restapi-mappings:
        id: product_id
        name: product_name
        price:
          sqlName: product_price
          sqlType: double      # Maps "999.99" (JSON string) to SQL DOUBLE
        quantity:
          sqlName: product_quantity
          sqlType: int         # Maps 10 (JSON integer) to SQL INTEGER
        created_at:
          sqlName: created_timestamp
          sqlType: timestamp   # Maps "2023-01-15T10:30:00Z" (JSON string) to SQL TIMESTAMP
        updated_at:
          sqlName: updated_timestamp
          sqlType: timestamp
        is_active:
          sqlName: is_active
          sqlType: boolean
        description: product_description
        weight:
          sqlName: product_weight
          sqlType: float
        category_id:
          sqlName: category_id
          sqlType: long        # Maps 1 (JSON integer) to SQL BIGINT
        rating:
          sqlName: product_rating
          sqlType: float
      properties: # ... (REST API field definitions)
```

**3. REST API Response (`products-all.json`)**

The mocked REST API returns product data. Notice `price` is a string and `category_id` is an integer, demonstrating the need for explicit type mapping.

```json
{
  "products": [
    {
      "id": "1",
      "name": "Laptop",
      "price": "999.99",
      "quantity": 10,
      "created_at": "2023-01-15T10:30:00Z",
      "updated_at": "2023-01-15T10:30:00Z",
      "is_active": true,
      "description": "High-performance laptop",
      "weight": 2.5,
      "category_id": 1,
      "rating": 4.5
    },
    {
      "id": "2",
      "name": "Mouse",
      "price": "25.99",
      "quantity": 50,
      "created_at": "2023-01-16T11:45:00Z",
      "updated_at": "2023-01-16T11:45:00Z",
      "is_active": true,
      "description": "Wireless mouse",
      "weight": 0.1,
      "category_id": 2,
      "rating": 4.2
    }
  ]
}
```

**4. SQL Query and Resulting Schema**

Executing a simple `SELECT *` query on the `products` table:

```sql
SELECT * FROM type_mappings_schema.products ORDER BY product_id
```

The adapter applies the defined mappings, and the SQL client (e.g., via `ResultSetMetaData`) perceives the following column types:

| Column Name       | SQL Type  | Java Type        |
| :---------------- | :-------- | :--------------- |
| product_id        | VARCHAR   | java.lang.String |
| product_name      | VARCHAR   | java.lang.String |
| product_price     | DOUBLE    | java.lang.Double |
| product_quantity  | INTEGER   | java.lang.Integer |
| created_timestamp | TIMESTAMP | java.sql.Timestamp |
| updated_timestamp | TIMESTAMP | java.sql.Timestamp |
| is_active         | BOOLEAN   | java.lang.Boolean |
| product_description | VARCHAR   | java.lang.String |
| product_weight    | REAL      | java.lang.Float  |
| category_id       | BIGINT    | java.lang.Long   |
| product_rating    | REAL      | java.lang.Float  |

This example demonstrates the power and flexibility of `x-calcite-restapi-mappings` for precise control over data types when integrating with diverse REST APIs.
```

**Result:** When querying the flattened table, you can use:
```sql
SELECT department_name, employee_name, task_title
FROM departments
WHERE department_budget >= 300000
```

The adapter automatically maps SQL column names to REST field names in filter requests:
- SQL: `WHERE department_name = 'IT'` → REST request: `{"name": "name", "operator": "=", "value": "IT"}`
- SQL: `WHERE department_budget >= 300000` → REST request: `{"name": "budget", "operator": ">=", "value": "300000"}`

**Test Coverage:**
- Nested structure mappings are tested in [NestedStructureJsonResponseTest.java](src/test/java/org/apache/calcite/adapter/restapi/tests/NestedStructureJsonResponseTest.java)
- Explicit SQL type mappings are tested in [TypeMappingsTest.java](src/test/java/org/apache/calcite/adapter/restapi/tests/TypeMappingsTest.java)

##### 8. `x-calcite-restapi-filterable-fields`
**Location:** operation level (per endpoint method)
**Type:** array of objects
**Description:** Explicitly defines which fields can be filtered when using POST operations with JSON request bodies. This extension is the **recommended approach** for POST operations to avoid semantic confusion with OpenAPI `parameters` (which suggest query strings).

**Why use this extension?**
- **Semantic clarity**: Makes it explicit that fields are filterable via JSON body (not query parameters)
- **No confusion**: Avoids misuse of OpenAPI `parameters` for POST operations
- **Type safety**: Allows explicit type declaration for each filterable field

**Format:**
```yaml
x-calcite-restapi-filterable-fields:
  - name: field_name
    type: string|integer|number|boolean
    format: optional_format  # e.g., "date-time" for timestamps
```

**Example:**
```yaml
paths:
  /users:
    post:
      operationId: users
      x-calcite-restapi-filterable-fields:
        - name: name
          type: string
        - name: age
          type: integer
        - name: department
          type: string
      requestBody:
        required: true
        content:
          application/json:
            schema:
              properties:
                name:
                  type: string
                age:
                  type: integer
                department:
                  type: string
      responses:
        '200':
          # response schema...
```

**Comparison with GET operations:**
- **GET operations**: Use standard OpenAPI `parameters` with `in: query` (semantically correct)
- **POST operations**: Use `x-calcite-restapi-filterable-fields` extension (avoids confusion)

**Test Coverage:**
- Filterable fields for POST are tested in [AllVariantsIntegrationTest.java](src/test/java/org/apache/calcite/adapter/restapi/tests/AllVariantsIntegrationTest.java)
- REQUEST-only parameters are tested in [RequestOnlyParametersTest.java](src/test/java/org/apache/calcite/adapter/restapi/tests/RequestOnlyParametersTest.java)

##### 9. Parameter Direction (REQUEST/RESPONSE/BOTH)

**Location:** Automatically determined from OpenAPI specification
**Type:** Auto-detected enum (`REQUEST`, `RESPONSE`, `BOTH`)
**Description:** The adapter automatically determines parameter direction based on where fields appear in the OpenAPI specification:

**Parameter Directions:**
- **REQUEST** - Parameter used only in request (appears in `parameters` or `requestBody`, NOT in response schema)
- **RESPONSE** - Parameter only in response (appears in response schema, NOT filterable)
- **BOTH** - Parameter in both request and response (appears in both locations)

**Automatic Detection:**
The adapter analyzes the OpenAPI specification to determine direction:

**How to Define Filterable Fields:**

The adapter supports **two approaches** depending on HTTP method:

**Option 1: GET operations (recommended for simple queries)**
Use standard OpenAPI `parameters` with `in: query`:
```yaml
paths:
  /users:
    get:  # GET operation
      operationId: users
      parameters:
        - name: name
          in: query
          description: Filter by name
          schema:
            type: string
        - name: age
          in: query
          description: Filter by age
          schema:
            type: integer
      responses:
        '200':
          # response schema...
```

**Option 2: POST operations (for complex filters in JSON body)**
Use extension `x-calcite-restapi-filterable-fields`:
```yaml
paths:
  /users:
    post:  # POST operation
      operationId: users
      # Define filterable fields using extension
      x-calcite-restapi-filterable-fields:
        - name: name
          type: string
        - name: age
          type: integer
        - name: department
          type: string
      requestBody:
        required: true
        content:
          application/json:
            schema:
              properties:
                name:
                  type: string
                age:
                  type: integer
                # ...
      responses:
        '200':
          # response schema...
```

**Why two approaches?**
- GET with `parameters` is **semantically correct** for OpenAPI (query parameters)
- POST with `x-calcite-restapi-filterable-fields` is **explicit** about adapter-specific behavior (filters in JSON body)
- Using standard `parameters` for POST would be misleading (suggests query string, but actually JSON body)

The adapter automatically detects parameter direction based on where fields appear:

1. **RESPONSE direction** - Field appears in response schema:
```yaml
responses:
  '200':
    content:
      application/json:
        schema:
          properties:
            companies:
              items:
                properties:
                  id:
                    type: integer
                  revenue:
                    type: number
```

2. **REQUEST direction** - Field appears in request parameters but NOT in response:
```yaml
parameters:
  - name: company_name
    in: query
    description: Search by company name
    schema:
      type: string

responses:
  '200':
    content:
      application/json:
        schema:
          properties:
            companies:
              items:
                properties:
                  id:
                    type: integer
                  # company_name NOT in response
```

3. **BOTH direction** - Field appears in both request parameters and response schema:
```yaml
# Field is filterable (defined in parameters section)
parameters:
  - name: name
    in: query
    schema:
      type: string
  - name: age
    in: query
    schema:
      type: integer

# AND appears in requestBody
requestBody:
  content:
    application/json:
      schema:
        properties:
          name:
            type: string
          age:
            type: integer

# AND appears in response
responses:
  '200':
    content:
      application/json:
        schema:
          properties:
            users:
              type: array
              items:
                properties:
                  id:
                    type: integer
                  name:
                    type: string
                  age:
                    type: integer
```

**Operator Validation for REQUEST-only Parameters:**

REQUEST-only parameters have special restrictions:

- **Allowed operators**: `=` (equality only)
  - Why: We can substitute the exact value from WHERE clause into result rows
  - Example: `WHERE company_name = 'Acme'` → All rows get `company_name = 'Acme'`

- **Prevented operators**: `>`, `<`, `<>`, `>=`, `<=`, `LIKE`, etc.
  - Why: We don't know actual values for each row (not in response)
  - Behavior: **Throws AssertionError** with helpful error message
  - Example: `WHERE company_name > 'A'` → Error: "Cannot execute query... Only '=' operator supported"

**Validation Logic:**
```java
// In FilterConverter.java
if (field.getDirection() == ApiParamDirection.REQUEST && !"=".equals(operator.getName())) {
    throw new AssertionError(
        String.format(
            "Cannot execute query: Field '%s' is REQUEST-only (not in response). " +
            "Only '=' operator supported. Current query uses '%s' which requires " +
            "actual field values from response for each row.",
            field.getName(), operator.getName()
        )
    );
}
```

**Example Scenario:**

**SQL Query:**
```sql
SELECT company_name, id, revenue
FROM companies
WHERE company_name = 'Acme Corp'
```

**OpenAPI Specification:**
```yaml
parameters:
  - name: company_name
    in: query
    schema:
      type: string

responses:
  '200':
    content:
      application/json:
        schema:
          properties:
            companies:
              items:
                properties:
                  id:
                    type: integer
                  revenue:
                    type: number
                  # company_name NOT here → REQUEST-only
```

**HTTP Request:**
```json
{
  "company_name": "Acme Corp"
}
```

**HTTP Response:**
```json
{
  "companies": [
    {"id": 1, "revenue": 5000000.0},
    {"id": 2, "revenue": 12000000.0}
  ]
}
```

**SQL Result (company_name substituted):**
```
company_name | id | revenue
Acme Corp    | 1  | 5000000.0
Acme Corp    | 2  | 12000000.0
```

**Key Implementation Details:**
- Substitution happens in `field.getRequestValue()` for REQUEST-only params
- BOTH params use actual values from response via `current.getArrayParamReader().read()`

**Test Coverage:**
- REQUEST-only parameter handling tested in [RequestOnlyParametersTest.java](src/test/java/org/apache/calcite/adapter/restapi/tests/RequestOnlyParametersTest.java)
- Covers `=` operator (works, value substituted), `>`, `<`, `<>` operators (throw clear error)

#### Complete Example with All Extensions

```yaml
openapi: 3.0.0
info:
  title: User Management API
  version: "1.0"
  x-calcite-restapi-schema-name: users_db
servers:
  - url: http://localhost:8080
paths:
  /users:
    post:
      operationId: getUserList
      x-calcite-restapi-table-name: users
      x-calcite-restapi-request-body-template: users-template.ftl
      x-calcite-restapi-pagination:
        pageStart: 1
        pageSize: 1000
      summary: Get users with DNF filters
      requestBody:
        required: true
        content:
          application/json:
            schema:
              type: object
              properties:
                page:
                  type: integer
                limit:
                  type: integer
                where:
                  type: array
                  items:
                    $ref: '#/components/schemas/FilterCriterion'
                or:
                  type: array
                  items:
                    type: array
                    items:
                      $ref: '#/components/schemas/FilterCriterion'
      responses:
        '200':
          description: User list
          content:
            application/json:
              schema:
                $ref: '#/components/schemas/UserList'
components:
  schemas:
    FilterCriterion:
      type: object
      properties:
        name:
          type: string
          enum: [name, age]
        operator:
          type: string
          enum: ["=", ">=", ">", "<", "<=", "!="]
        value:
          oneOf:
            - type: string
            - type: integer
    UserList:
      type: object
      properties:
        users:
          type: array
          items:
            $ref: '#/components/schemas/User'
    User:
      type: object
      properties:
        name:
          type: string
          x-calcite-restapi-field-name: name
        age:
          type: integer
          x-calcite-restapi-field-name: age
```

---

## Macro Substitutions

Macros can be used in URL, headers, and body.  
The [FreeMarker](https://freemarker.apache.org/) engine powers macro support. 

Built-in macros:

- `name` – table name
- `user` – username used in JDBC url
- `password` – password used in JDBC url
- `offset` – auto-incremented by page-size for each REST call
- `limit` – page size, defined in XML as page-size
- `projects` – key-value structure holding all fields used in SELECT сlause of query (`${projects.<name>}`)
- `filters_dnf` – list of DNF (disjunctive normal form) condition groups
- `filters_cnf` – list of CNF (conjunctive normal form) condition groups

Also, any parameters passed in the Calcite JDBC connection URL are available. In example there is `${restApiVersion}`, `${contentType}`, `${jwtToken}`

---

## WHERE Clause

The SQL query can use filtering, which Calcite will apply to REST data.  
If REST service supports filters, they can be sent in requests (see main example); all filters are in the `filters_dnf` and `filters_cnf` parameters.

Operators in SQL can be: `=`, `>`, `<`, `>=`, `<=`, `<>`.  
If the REST service can't filter, simply don't use filters in the template; Calcite will filter locally.

---

## Field Selection (Projection Pushdown)

You may select individual fields in SQL, e.g.:

```sql
SELECT name, age FROM users WHERE age >= 25
```

All selected fields will show up in the `projects` macro (keys are field names).

The adapter optimizes queries by sending only the requested columns to the REST API (projection pushdown). This reduces network traffic and improves performance by avoiding unnecessary data transfer.

**Example: Projection Pushdown with Filters**

This example demonstrates how the adapter intelligently pushes down selected columns and filter conditions to the REST API, resulting in an optimized HTTP request.

The complete working example can be found in the [ProjectionsTest.java](src/test/java/org/apache/calcite/adapter/restapi/tests/ProjectionsTest.java) test, specifically in the `testProjectionWithFilter` method.

**1. The Goal**

We want to retrieve only the `name` and `last_name` of users who are 23 years or older using a standard SQL query.

```sql
SELECT name, last_name
FROM users_schema.users
WHERE age >= 23
```

**2. OpenAPI Specification (`projections.yaml`)**

The OpenAPI definition specifies the REST endpoint for users and points to a FreeMarker template (`body-projections-json-template.ftl`) responsible for constructing the HTTP request body. It also defines how SQL column names (e.g., `last_name`) map to REST API field names (e.g., `lastName`).

```yaml
# src/test/resources/rest/ProjectionsTest/projections.yaml
paths:
  /users:
    post:
      operationId: users
      summary: Get users with projection and filter support
      x-calcite-restapi-table-name: users
      x-calcite-restapi-request-body-template: body-projections-json-template.ftl
      ...
components:
  schemas:
    User:
      type: object
      x-calcite-restapi-mappings:
        id: id
        name: name
        lastName: last_name # SQL column 'last_name' maps to REST field 'lastName'
        age: age
        department: department
      properties: ...
```

**3. FreeMarker Request Body Template (`body-projections-json-template.ftl`)**

This FreeMarker template dynamically constructs the JSON request body. It intelligently includes the `projections` array (containing only the fields requested in the SQL `SELECT` clause) and the `filters` array (derived from the SQL `WHERE` clause).

```freemarker
<#-- src/test/resources/rest/ProjectionsTest/body-projections-json-template.ftl -->
{
  "page": ${(offset / limit)?int + pageStart},
  "limit": ${limit}<#if projects?has_content>,
  "projections": [
    <#list projects as restField>
    "${restField}"<#if restField?has_next>,</#if>
    </#list>
  ]</#if><#if filters_dnf?has_content && (filters_dnf?size > 0)>,
  "filters": [
    <#list filters_dnf as andGroup>
    [
      <#list andGroup as criterion>
      {
        "name": "${criterion.name}",
        "operator": "${criterion.operator}",
        "value": <#if criterion.value?is_number>${criterion.value}<#else>"${criterion.value}"</#if>
      }<#if criterion?has_next>,</#if>
      </#list>
    ]<#if andGroup?has_next>,</#if>
    </#list>
  ]</#if>
}
```

**4. Generated HTTP Request Body**

When the SQL query `SELECT name, last_name FROM users_schema.users WHERE age >= 23` is executed, the adapter generates the following HTTP `POST` request body:

```json
POST /users HTTP/1.1
Content-Type: application/json

{
  "page": 1,
  "limit": 100,
  "projections": [
    "name",
    "lastName"
  ],
  "filters": [
    [
      {
        "name": "age",
        "operator": ">=",
        "value": 23
      }
    ]
  ]
}
```
*Notice how only `name` and `lastName` (mapped from `last_name`) are requested as projections, and the `age >= 23` filter is included in the request, demonstrating effective pushdown.*

**5. Result Set**

The adapter processes the API response and returns the flattened data, providing only the columns explicitly requested:

| name   | last_name |
| :----- | :-------- |
| Alice  | Smith     |
| Bob    | Smith     |
| John   | Doe       |
| Martin | Smith     |
| Sarah  | Smith     |

This example highlights how Projection Pushdown minimizes data transfer and offloads filtering logic to the REST API when supported, enhancing query performance.

**Test Coverage:** Projection pushdown is tested in [ProjectionsTest.java](src/test/java/org/apache/calcite/adapter/restapi/tests/ProjectionsTest.java), including:
- SELECT specific columns with filters
- SELECT * (all columns)
- Projection without filters
- Verification that only requested columns are sent to REST API

---

## JOIN Operations

The adapter fully supports SQL JOIN operations, allowing you to combine data from multiple REST API endpoints or perform self-joins on a single table. Calcite fetches data from REST APIs and performs JOIN operations in memory.

### Supported JOIN Types

- **INNER JOIN** - Returns records with matching values in both tables
- **LEFT JOIN** - Returns all records from the left table and matched records from the right
- **RIGHT JOIN** - Returns all records from the right table and matched records from the left
- **FULL OUTER JOIN** - Returns all records when there is a match in either table
- **CROSS JOIN** - Returns the Cartesian product of both tables
- **SELF JOIN** - Joins a table to itself

### Example 1: Self-JOIN to Find Colleagues

Find pairs of users working in the same department:

```sql
SELECT
    u1.id as user1_id,
    u1.name as user1_name,
    u2.id as user2_id,
    u2.name as user2_name,
    u1.department
FROM users_schema.users u1
JOIN users_schema.users u2
    ON u1.department = u2.department
WHERE u1.id < u2.id
ORDER BY u1.id, u2.id
```

**Result:** Returns pairs of users (colleagues) who work in the same department.

### Example 2: Self-JOIN with Aggregation

Count how many colleague pairs exist in each department:

```sql
SELECT
    u1.department,
    COUNT(*) as colleague_pairs
FROM users_schema.users u1
JOIN users_schema.users u2
    ON u1.department = u2.department
WHERE u1.id < u2.id
GROUP BY u1.department
ORDER BY colleague_pairs DESC
```

**Result:** Returns departments with the count of colleague pairs.

### Example 3: CROSS JOIN

Create all possible combinations of users (Cartesian product):

```sql
SELECT
    u1.id as user1_id,
    u1.name as user1_name,
    u2.id as user2_id,
    u2.name as user2_name
FROM users_schema.users u1
CROSS JOIN users_schema.users u2
WHERE u1.id < u2.id
ORDER BY u1.id, u2.id
LIMIT 10
```

**Result:** Returns up to 10 pairs of different users.

### How JOINs Work with REST APIs

1. **Data Fetching**: Calcite fetches complete datasets from each REST API endpoint involved in the JOIN
2. **In-Memory Processing**: JOIN operations are performed by Calcite in memory after data retrieval
3. **Filter Pushdown**: WHERE clauses that don't involve JOIN conditions are pushed to REST APIs when possible
4. **Optimization**: Calcite's query optimizer handles JOIN order and execution strategy

### Important Notes

- **Performance**: JOINs are performed in-memory, so be mindful of dataset sizes
- **Pagination**: Each table is fetched with pagination settings defined in OpenAPI spec
- **Multiple Endpoints**: You can JOIN data from different REST API endpoints within the same schema or across schemas
- **Filter Optimization**: Filters on individual tables (before JOIN) are pushed to REST APIs; JOIN conditions are evaluated locally

### Test Coverage

For complete working examples, see:
- **JoinTest.java** - Comprehensive JOIN test suite including:
  - Self-JOIN for finding colleagues (`testSelfJoinFindColleagues`)
  - Self-JOIN with aggregation (`testSelfJoinWithAggregation`)
  - CROSS JOIN examples (`testCrossJoin`)

All JOIN tests use the same mock REST API infrastructure and demonstrate real-world JOIN scenarios.

---

## FreeMarker Syntax

See [Apache Freemarker documentation](https://freemarker.apache.org/docs/dgui_template_exp.html#dgui_template_exp_arit). Below are the basics:

| Description                             | Usage                                                                             |
|-----------------------------------------|-----------------------------------------------------------------------------------|
| Simple apiParameter usage (e.g. limit)     | `${limit}`                                                                        |
| Key-value structure (e.g. projects map) | `${projects.name}`<br/>`${projects.age}`                                          |
| Assign variable (take first list group) | `<#assign firstGroup = filters_dnf[0] />`                                         |
| Check if list has content               | `<#if filters_dnf?has_content>...</#if>`                                              |
| Access criterion field values           | `${criterion[0].name}`<br/>`${criterion[0].operator}`<br/>`${criterion[0].value}` |
| Check if list size > 1                  | `<#if (filters_dnf?size > 1)>...</#if>`                                               |
| Iterate list (`?has_next` for comma)    | `<#list filters_dnf as orGroup>{...}<#if orGroup?has_next>,</#if></#list>`            |
| Integer division                        | `${(offset / limit)?int}`                                                         |
| Return error on condition               | `<#if (filters_dnf?size > 1)><#stop "Error: ..."></#if>`                              |

---

## Paging Mechanism

Paging is enabled by `page-size > 0`, resulting in multiple REST calls with macros `${limit}` and `${offset}`.  
Paging stops when a REST reply contains less elements than page-size.

:warning: The current implementation does not natively support mapping SQL OFFSET and LIMIT clauses to REST requests.  
LIMIT is handled through paginated requests until the required number of records is retrieved,  
while SQL OFFSET is applied only client-side after fetching all records from the REST source.  
This limitation arises because the method  
```java
Enumerable<@Nullable Object[]> scan(DataContext root, List<RexNode> filters, int @Nullable[] projects)
```
of the 

```java
org.apache.calcite.schema.ProjectableFilterableTable
```
interface does not receive the OFFSET and LIMIT values specified in the SQL query.

**Example: Automatic Pagination for Fetching All Records**

This example demonstrates how the adapter automatically handles pagination by making multiple requests to a REST API until all available data is retrieved.

The complete working example can be found in the [PaginationTest.java](src/test/java/org/apache/calcite/adapter/restapi/tests/PaginationTest.java) test, specifically in the `testFetchAllDataWithPagination` method.

**1. The Goal**

We want to retrieve all user records from a REST API that paginates its responses, where each page returns a maximum of 2 users.

**2. OpenAPI Specification (`pagination.yaml`)**

The OpenAPI definition for the `/users` endpoint configures pagination parameters using the `x-calcite-restapi-pagination` extension, specifying that the API starts from `page 1` and returns `pageSize` (limit) of `2` items per request. It also links to a FreeMarker template for the request body.

```yaml
# src/test/resources/rest/PaginationTest/pagination.yaml
paths:
  /users:
    post:
      operationId: users
      x-calcite-restapi-table-name: users
      x-calcite-restapi-request-body-template: body-pagination-json-template.ftl
      x-calcite-restapi-pagination: # Pagination configuration
        pageStart: 1              # Start from page 1
        pageSize: 2               # Fetch 2 records per page
      requestBody:
        required: true
        content:
          application/json:
            schema:
              type: object
              properties:
                page:
                  type: integer   # API expects 'page' parameter
                limit:
                  type: integer   # API expects 'limit' parameter
              additionalProperties: false
```

**3. FreeMarker Request Body Template (`body-pagination-json-template.ftl`)**

This simple FreeMarker template constructs the JSON request body, passing the dynamically calculated `page` number and the configured `limit` (page size) to the REST API.

```freemarker
<#-- src/test/resources/rest/PaginationTest/body-pagination-json-template.ftl -->
{
  "page": ${(offset / limit)?int + pageStart},
  "limit": ${limit}
}
```

**4. SQL Query**

A standard SQL `SELECT *` query is executed. The adapter transparently handles the multiple paginated requests to the REST API.

```sql
SELECT * FROM users_schema.users ORDER BY id
```

**5. Mocked API Responses (Simplified)**

The REST API is mocked to return 7 users distributed across 4 pages, with the last page containing fewer than the maximum items, signaling the end of data.

*   Page 1 (users 1, 2)
*   Page 2 (users 3, 4)
*   Page 3 (users 5, 6)
*   Page 4 (user 7) - *This page has only 1 user, which is less than `pageSize=2`, so the adapter stops fetching.*

**6. Generated HTTP Requests**

When the SQL query is executed, the adapter makes a sequence of `POST` requests to the `/users` endpoint:

*   **Request 1:** `POST /users` with body `{ "page": 1, "limit": 2 }`
*   **Request 2:** `POST /users` with body `{ "page": 2, "limit": 2 }`
*   **Request 3:** `POST /users` with body `{ "page": 3, "limit": 2 }`
*   **Request 4:** `POST /users` with body `{ "page": 4, "limit": 2 }`

A total of 4 requests are made, each fetching a page of data.

**7. Result Set**

The adapter combines the data from all fetched pages and presents it as a single, unified result set to the SQL client:

| id | name    | age | department |
|----|---------|-----|------------|
| 1  | Alice   | 25  | IT         |
| 2  | Bob     | 30  | HR         |
| 3  | Charlie | 22  | Sales      |
| 4  | Diana   | 28  | IT         |
| 5  | Eve     | 35  | Marketing  |
| 6  | Frank   | 40  | HR         |
| 7  | Grace   | 30  | Sales      |

This example clearly illustrates how the adapter automates the complex task of fetching data from paginated REST APIs, making it appear as a single, continuous stream to the SQL user.

**Test Coverage:** Pagination behavior is tested in [PaginationTest.java](src/test/java/org/apache/calcite/adapter/restapi/tests/PaginationTest.java), including:
- Fetching all records with pagination (multiple REST requests)
- SQL LIMIT optimization (stops fetching when limit is reached)
- ORDER BY impact on pagination (fetches all data for sorting)

---

## JSON and XML Response Structure Handling

The adapter supports different JSON and XML response structure formats, allowing flexible integration with various REST API designs.

### Supported Response Structure Types

#### 1. Anonymous Array Response (Direct Array Without Wrapping Object)

**Description:** REST API returns a direct array without any wrapping object. Each element in the array becomes a row in the SQL table.

**JSON Example:**
```json
[
  {"id": 1, "name": "Item 1", "value": "Value 1"},
  {"id": 2, "name": "Item 2", "value": "Value 2"},
  {"id": 3, "name": "Item 3", "value": "Value 3"}
]
```

**XML Example:**
```xml
<items>
  <item>
    <id>1</id>
    <name>Item 1</name>
    <item_value>Value 1</item_value>
  </item>
  <item>
    <id>2</id>
    <name>Item 2</name>
    <item_value>Value 2</item_value>
  </item>
  <item>
    <id>3</id>
    <name>Item 3</name>
    <item_value>Value 3</item_value>
  </item>
</items>
```

**SQL Query:**
```sql
SELECT id, name, item_value
FROM anonymous_array_schema.data
ORDER BY id
```

**Result:** Returns 3 rows with direct access to element properties.

#### 2. Single Object Response (No Array)

**Description:** REST API returns a single object instead of an array. This is treated as a table with one row.

**JSON Example:**
```json
{
  "id": 50,
  "name": "Standalone Item",
  "description": "This is a standalone item in a single object response",
  "status": "active"
}
```

**XML Example:**
```xml
<item>
  <id>50</id>
  <name>Standalone Item</name>
  <description>This is a standalone item in a single object response</description>
  <status>active</status>
</item>
```

**SQL Query:**
```sql
SELECT id, name, description, status
FROM single_row_schema.single
```

**Result:** Returns 1 row with all object properties.

#### 3. Nested Array Response with Root Fields

**Description:** REST API returns a complex object with root-level fields and nested arrays. Root fields repeat for each item in nested arrays, allowing denormalized access to hierarchical data.

**JSON Example:**
```json
{
  "response_id": 12345,
  "response_timestamp": "2023-12-20T10:00:00Z",
  "response_source": "product_catalog_api",
  "categories": [
    {
      "category_id": 1,
      "category_name": "Electronics",
      "items": [
        {"id": 101, "name": "Laptop", "price": 999.99},
        {"id": 102, "name": "Mouse", "price": 25.99}
      ]
    },
    {
      "category_id": 2,
      "category_name": "Books",
      "items": [
        {"id": 201, "name": "Java Guide", "price": 49.99},
        {"id": 202, "name": "Python Basics", "price": 39.99}
      ]
    }
  ]
}
```

**XML Example:**
```xml
<response>
  <response_id>12345</response_id>
  <response_timestamp>2023-12-20T10:00:00Z</response_timestamp>
  <response_source>product_catalog_api</response_source>
  <categories>
    <category>
      <category_id>1</category_id>
      <category_name>Electronics</category_name>
      <items>
        <item>
          <id>101</id>
          <name>Laptop</name>
          <price>999.99</price>
        </item>
        <item>
          <id>102</id>
          <name>Mouse</name>
          <price>25.99</price>
        </item>
      </items>
    </category>
    <category>
      <category_id>2</category_id>
      <category_name>Books</category_name>
      <items>
        <item>
          <id>201</id>
          <name>Java Guide</name>
          <price>49.99</price>
        </item>
        <item>
          <id>202</id>
          <name>Python Basics</name>
          <price>39.99</price>
        </item>
      </items>
    </category>
  </categories>
</response>
```

**SQL Query:**
```sql
SELECT response_id, response_timestamp, response_source,
       category_id, category_name,
       item_id, item_name, item_price
FROM fields_array_schema.complex
ORDER BY category_id, item_id
```

**Result:** Returns 4 rows where root fields (`response_id`, `response_timestamp`, `response_source`) and category fields (`category_id`, `category_name`) repeat for each item in nested arrays:

| response_id | response_timestamp | response_source | category_id | category_name | item_id | item_name | item_price |
|-------------|-------------------|-----------------|-------------|---------------|---------|-----------|------------|
| 12345 | 2023-12-20T10:00:00Z | product_catalog_api | 1 | Electronics | 101 | Laptop | 999.99 |
| 12345 | 2023-12-20T10:00:00Z | product_catalog_api | 1 | Electronics | 102 | Mouse | 25.99 |
| 12345 | 2023-12-20T10:00:00Z | product_catalog_api | 2 | Books | 201 | Java Guide | 49.99 |
| 12345 | 2023-12-20T10:00:00Z | product_catalog_api | 2 | Books | 202 | Python Basics | 39.99 |

**OpenAPI Configuration:**

For nested array responses, the path in the OpenAPI specification determines how nested arrays are flattened:

```yaml
# For JSON response with nested arrays
responses:
  '200':
    content:
      application/json:
        schema:
          $ref: '#/components/schemas/ResponseWithNestedArrays'

# For XML response with nested arrays
responses:
  '200':
    content:
      application/xml:
        schema:
          $ref: '#/components/schemas/ResponseWithNestedArrays'
```

The path `$.categories.items` flattens nested arrays by:
1. Iterating through the `categories` array
2. For each category, accessing its `items` array
3. Creating one row for each item with all parent data merged in

**Field Mappings:**

Use `x-calcite-restapi-mappings` to map REST field names to SQL column names:

```yaml
ItemInCategory:
  type: object
  x-calcite-restapi-mappings:
    id: item_id        # REST field 'id' maps to SQL column 'item_id'
    name: item_name    # REST field 'name' maps to SQL column 'item_name'
    price: item_price  # REST field 'price' maps to SQL column 'item_price'
  properties:
    id:
      type: integer
    name:
      type: string
    price:
      type: number
```

**Test Coverage:**

JSON and XML response structure handling is tested in [ResponseStructureTest.java](src/test/java/org/apache/calcite/adapter/restapi/tests/ResponseStructureTest.java), including:

- **Anonymous Array Response Tests:**
  - `testAnonymousJsonArrayResponse()` - Direct JSON arrays without wrapping objects
  - `testAnonymousXmlArrayResponse()` - Direct XML arrays without wrapping objects

- **Single Object Response Tests:**
  - `testSingleObjectResponse()` - Single JSON objects as single-row tables
  - `testSingleXmlObjectResponse()` - Single XML objects as single-row tables

- **Nested Array Response Tests:**
  - `testFieldsWithArrayResponse()` - JSON with root fields and nested arrays
  - `testXmlFieldsWithArrayResponse()` - XML with root fields and nested arrays

All tests verify that the adapter correctly handles different response structures and that root-level fields properly repeat for each item in nested arrays.

---


## Usage

### Basic Setup

The adapter uses OpenAPI 3.0 specifications (YAML format) to define REST API endpoints and map them to SQL tables. Here's a complete working example:

#### 1. Create OpenAPI Specification

Create an OpenAPI YAML file (e.g., `my-api.yaml`) describing your REST API:

```yaml
openapi: 3.0.0
info:
  title: My REST API
  version: "1.0"
  x-calcite-restapi-schema-name: my_schema  # SQL schema name
servers:
  - url: http://localhost:8080
paths:
  /users:
    post:
      operationId: getUsers
      summary: Get users
      x-calcite-restapi-table-name: users  # SQL table name
      x-calcite-restapi-request-body-template: users-request.ftl  # Request body template
      x-calcite-restapi-pagination:
        pageStart: 1
        pageSize: 100
      responses:
        '200':
          description: User list
          content:
            application/json:
              schema:
                $ref: '#/components/schemas/UserList'
components:
  schemas:
    UserList:
      type: object
      properties:
        users:
          type: array
          items:
            $ref: '#/components/schemas/User'
    User:
      type: object
      x-calcite-restapi-mappings:  # Maps REST fields to SQL columns
        id: id
        name: name
        lastName: last_name  # lastName (REST) → last_name (SQL)
        age: age
      properties:
        id:
          type: integer
        name:
          type: string
        lastName:
          type: string
        age:
          type: integer
```

#### 2. Create FreeMarker Request Body Template

Create a FreeMarker template file (e.g., `users-request.ftl`) to generate HTTP request bodies:

```ftl
{
  "page": ${(offset / limit)?int},
  "limit": ${limit}
  <#if filters?has_content>
  ,"filters": [
    <#list filters as filter>
    {"name": "${filter.name}", "operator": "${filter.operator}", "value": "${filter.value}"}<#if filter?has_next>,</#if>
    </#list>
  ]
  </#if>
  <#if projections?has_content>
  ,"projections": [
    <#list projections as projection>
    "${projection}"<#if projection?has_next>,</#if>
    </#list>
  ]
  </#if>
}
```

#### 3. Java Code to Query REST API via SQL

```java
import java.sql.*;
import java.util.Properties;
import org.apache.calcite.jdbc.CalciteConnection;
import org.apache.calcite.adapter.restapi.rest.RestSchemaFactory;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.schema.SchemaFactory;

public class RestApiExample {

    public static void main(String[] args) {
        // Set path to directory containing OpenAPI YAML files and FreeMarker templates
        System.setProperty("calcite.rest", "/path/to/openapi/specs");

        try (Connection connection = getConnection();
             Statement statement = connection.createStatement()) {

            // Query REST API using SQL
            String sql = """
                SELECT id, name, last_name, age
                FROM my_schema.users
                WHERE age >= 25
                ORDER BY id
                """;

            ResultSet rs = statement.executeQuery(sql);

            while (rs.next()) {
                int id = rs.getInt("id");
                String name = rs.getString("name");
                String lastName = rs.getString("last_name");
                int age = rs.getInt("age");

                System.out.printf("User %d: %s %s (age %d)%n",
                    id, name, lastName, age);
            }

        } catch (SQLException e) {
            throw new RuntimeException("SQL error", e);
        }
    }

    private static Connection getConnection() throws SQLException {
        // Set Calcite connection properties
        Properties properties = new Properties();
        properties.setProperty("lex", "JAVA");  // SQL lexer type

        // Optional: Pass variables for FreeMarker templates (e.g., auth tokens)
        properties.setProperty("jwtToken", "YOUR_JWT_TOKEN_HERE");
        properties.setProperty("apiVersion", "v2.0");

        // Register Calcite JDBC driver
        DriverManager.registerDriver(new org.apache.calcite.jdbc.Driver());

        // Create Calcite connection
        CalciteConnection calciteConnection =
            DriverManager.getConnection("jdbc:calcite:", properties)
                .unwrap(CalciteConnection.class);

        // Register REST schema factory
        SchemaPlus rootSchema = calciteConnection.getRootSchema();
        SchemaFactory restSchemaFactory = new RestSchemaFactory();

        // Create REST schema (loads all OpenAPI specs from calcite.rest directory)
        org.apache.calcite.adapter.restapi.rest.RestSchema restSchema =
            (org.apache.calcite.adapter.restapi.rest.RestSchema)
            restSchemaFactory.create(rootSchema, "", new java.util.HashMap<>());

        // Register all schemas from OpenAPI specs
        restSchema.registerSubSchemas(rootSchema);

        return calciteConnection;
    }
}
```

### Configuration Options

#### System Properties

- **`calcite.rest`**: Path to directory containing OpenAPI YAML files and FreeMarker templates (required)
  ```bash
  -Dcalcite.rest=/path/to/specs
  ```

- **Fallback**: If `calcite.rest` is not set, adapter searches for `catalina.base` (Tomcat) and looks for `calcite/rest` directory

#### Connection Properties

Pass FreeMarker template variables via connection properties:

```java
Properties properties = new Properties();
properties.setProperty("lex", "JAVA");

// Template variables (accessible in .ftl files)
properties.setProperty("jwtToken", "Bearer eyJ0eXAiOi...");
properties.setProperty("apiVersion", "v2.0");
properties.setProperty("contentType", "application/json");
```

Use in FreeMarker template:
```ftl
Authorization: ${jwtToken}
API-Version: ${apiVersion}
Content-Type: ${contentType}
```

### Directory Structure

```
calcite/rest/
├── my-api.yaml                 # OpenAPI specification
├── users-request.ftl           # Request body template for /users
├── products-request.ftl        # Request body template for /products
└── custom-headers.ftl          # Optional: custom headers template
```

### Dependencies

Add the following dependencies to your project (Maven example):

```xml
<dependencies>
    <!-- Calcite REST API Adapter -->
    <dependency>
        <groupId>org.apache.calcite.adapter.restapi</groupId>
        <artifactId>calcite-restapi-adapter</artifactId>
        <version>1.0.0</version>
    </dependency>

    <!-- Calcite Core -->
    <dependency>
        <groupId>org.apache.calcite</groupId>
        <artifactId>calcite-core</artifactId>
        <version>1.41.0</version>
    </dependency>

    <!-- FreeMarker for templates -->
    <dependency>
        <groupId>org.freemarker</groupId>
        <artifactId>freemarker</artifactId>
        <version>2.3.34</version>
    </dependency>

    <!-- Jackson for JSON/XML/YAML processing -->
    <dependency>
        <groupId>com.fasterxml.jackson.core</groupId>
        <artifactId>jackson-databind</artifactId>
        <version>2.18.4</version>
    </dependency>
    <dependency>
        <groupId>com.fasterxml.jackson.dataformat</groupId>
        <artifactId>jackson-dataformat-yaml</artifactId>
        <version>2.18.4</version>
    </dependency>
    <dependency>
        <groupId>com.fasterxml.jackson.dataformat</groupId>
        <artifactId>jackson-dataformat-xml</artifactId>
        <version>2.18.4</version>
    </dependency>
</dependencies>
```

For complete dependency list, see `pom.xml` in the project.

### Complete Working Example

See test classes for complete working examples:
- **[JoinTest.java](src/test/java/org/apache/calcite/adapter/restapi/tests/JoinTest.java)** - JOIN operations, filter/projection pushdown
- **[PaginationTest.java](src/test/java/org/apache/calcite/adapter/restapi/tests/PaginationTest.java)** - Automatic pagination
- **[RequestHeadersTest.java](src/test/java/org/apache/calcite/adapter/restapi/tests/RequestHeadersTest.java)** - Custom HTTP headers
- **[ProjectionsTest.java](src/test/java/org/apache/calcite/adapter/restapi/tests/ProjectionsTest.java)** - Column selection optimization

All test resources (OpenAPI specs, FreeMarker templates, response files) are located in:
```
src/test/resources/rest/[TestClassName]/
```

## Contact

If you have questions about this project or need help with configuration, feel free to contact:

- Oleg Alekseev
- Telegram: [@oalekseevdev](https://t.me/oalekseevdev)
- E-mail: [o_alekseev@mail.ru](mailto:o_alekseev@mail.ru)