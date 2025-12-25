<?xml version="1.0" encoding="UTF-8"?>
<Request><#if limit gt 0>
  <page>${(offset / limit)?int + pageStart}</page>
  <limit>${limit}</limit></#if><#if projects?has_content>
  <projections><#list projects as restField>
    <field>${restField}</field></#list>
  </projections></#if><#if filters_dnf?has_content && (filters_dnf?size > 0)>
  <filters><#list filters_dnf as andGroup>
    <group><#list andGroup as criterion>
      <criterion>
        <name>${criterion.name}</name>
        <operator>${criterion.operator}</operator>
        <value><#if criterion.value?is_number>${criterion.value}<#else>${criterion.value}</#if></value>
      </criterion></#list>
    </group></#list>
  </filters></#if>
</Request>