{
  "page": ${(offset / limit)?int + pageStart},
  "limit": ${limit}<#if filters_dnf?has_content && (filters_dnf?size > 0)>,
  <#list filters_dnf[0] as criterion>
  <#if (criterion.name == 'rating')>"rating": ${criterion.value}</#if>
  </#list></#if>
}