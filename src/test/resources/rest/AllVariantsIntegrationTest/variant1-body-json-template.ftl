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
