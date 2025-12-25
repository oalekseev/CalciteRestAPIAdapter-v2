{
  "page": ${(offset / limit)?int},
  "limit": ${limit}<#if filters_cnf?has_content && (filters_cnf?size > 0)>,
  <#-- Variant 5: CNF with 'filters' field only -->
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