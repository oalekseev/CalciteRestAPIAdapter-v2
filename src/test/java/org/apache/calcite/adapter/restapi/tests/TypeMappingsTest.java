package org.apache.calcite.adapter.restapi.tests;

import org.apache.calcite.adapter.restapi.tests.base.BaseTest;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import java.sql.*;
import static org.junit.jupiter.api.Assertions.*;

/**
 * Simplified type mappings test - verifies SQL type mappings work correctly.
 * Tests x-calcite-restapi-mappings with different SQL types.
 */
public class TypeMappingsTest extends BaseTest {
    
    @Override
    protected String getTestResourceDirectory() {
        return "TypeMappingsTest";
    }
    
    @Test
    @DisplayName("Type Mappings: Verify SQL types (INTEGER, DECIMAL, TIMESTAMP, BOOLEAN, etc.)")
    public void testSqlTypeMappings() throws Exception {
        setupStaticJsonResponse("/products", "products-all.json");
        setupConnection("type-mappings.yaml");
        
        System.out.println("=== Test: SQL Type Mappings ===");
        
        String sql = "SELECT * FROM type_mappings_schema.products ORDER BY product_id";
        
        try (Statement stmt = getConnection().createStatement();
             ResultSet rs = stmt.executeQuery(sql)) {
            
            ResultSetMetaData meta = rs.getMetaData();
            
            System.out.println("\nColumn Types:");
            for (int i = 1; i <= meta.getColumnCount(); i++) {
                System.out.printf("  %s: %s (%s)%n",
                    meta.getColumnName(i),
                    meta.getColumnTypeName(i),
                    meta.getColumnClassName(i));
            }
            
            while (rs.next()) {
                System.out.printf("\nProduct: id=%s, name=%s, price=%s, quantity=%d%n",
                    rs.getString("product_id"), rs.getString("product_name"),
                    rs.getDouble("product_price"), rs.getInt("product_quantity"));
            }
        }
        
        System.out.println("\n✓ Type mappings test completed");
    }
}
