package org.apache.calcite.adapter.restapi.tests;

import org.apache.calcite.adapter.restapi.tests.base.BaseTest;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import java.sql.*;

/**
 * Simplified nested structure XML test - verifies XML parsing of nested data.
 * Tests Departments → Employees → Tasks flattening in XML format.
 */
public class NestedStructureXmlTest extends BaseTest {
    
    @Override
    protected String getTestResourceDirectory() {
        return "NestedStructureXmlTest";
    }
    
    @Test
    @DisplayName("Nested XML: Departments → Employees → Tasks flattening")
    public void testNestedXmlStructure() throws Exception {
        setupStaticXmlResponse("/departments", "departments-all.xml");
        setupConnection("neested-xml-response.yaml");
        
        System.out.println("=== Test: Nested XML Structure (Departments → Employees → Tasks) ===");
        
        String sql = "SELECT * FROM departments_schema.departments ORDER BY department_id";
        
        int rowCount = 0;
        try (Statement stmt = getConnection().createStatement();
             ResultSet rs = stmt.executeQuery(sql)) {
            
            while (rs.next()) {
                rowCount++;
                System.out.printf("Row %d: Dept[%d:%s] → Employee[%d:%s] → Task[%d:%s]%n",
                    rowCount,
                    rs.getInt("department_id"), rs.getString("department_name"),
                    rs.getInt("employee_id"), rs.getString("employee_name"),
                    rs.getInt("task_id"), rs.getString("task_title"));
            }
        }
        
        System.out.printf("\n✓ XML nested structure test completed: %d flattened rows%n", rowCount);
    }
}
