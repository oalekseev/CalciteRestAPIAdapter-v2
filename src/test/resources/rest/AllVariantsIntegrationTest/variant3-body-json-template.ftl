{
  "page": ${(offset / limit)?int},
  "limit": ${limit}<#if filters_cnf?has_content && (filters_cnf?size > 0)>,
  <#-- Variant 3: CNF with 'where' (first OR group) + 'and' (remaining AND groups) -->
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