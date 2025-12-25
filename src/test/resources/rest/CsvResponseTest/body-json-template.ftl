{
  "page": ${(offset / limit)?int},
  "limit": ${limit}<#if filters_dnf?has_content && (filters_dnf?size > 0)>,
  <#-- DNF with 'where' (first AND group) + 'or' (remaining OR groups) -->
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
