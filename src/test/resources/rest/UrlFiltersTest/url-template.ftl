<#-- URL template that supports filters, projections, and pagination as query parameters -->
<#setting url_escaping_charset='UTF-8'>
<#if filters_dnf?has_content && (filters_dnf?size > 0)>
  <#-- Error if more than one AND group (which would indicate OR operations) -->
  <#if (filters_dnf?size > 1)><#stop "Error: URL template does not support OR operators (DNF with multiple AND groups)"></#if>
  <#-- Error if any criterion uses an unsupported operator -->
  <#list filters_dnf[0] as criterion>
    <#if (criterion.operator != '=')><#stop "Error: URL template only supports '=' operator. Found operator: '${criterion.operator}'"></#if>
  </#list>
</#if>
/users?<#if offset gte 0>offset=${offset}</#if><#if limit gt 0>&limit=${limit}</#if><#if projects?has_content><#list projects as proj>&proj=${proj?url}</#list></#if><#if filters_dnf?has_content && (filters_dnf?size > 0)><#list filters_dnf as andGroup><#list andGroup as criterion>&${criterion.name?url}=${criterion.value?string?url}</#list></#list></#if>