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