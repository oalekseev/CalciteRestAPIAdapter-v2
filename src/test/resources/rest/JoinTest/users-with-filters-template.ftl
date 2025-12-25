{
  "page": ${(offset / limit)?int},
  "limit": ${limit}<#if filters_dnf?has_content && (filters_dnf?size > 0)>,
  "filters_dnf": [
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
  ]</#if><#if projects?has_content>,
  "projections": [<#list projects as proj>"${proj}"<#if proj?has_next>,</#if></#list>]
  </#if>
}
