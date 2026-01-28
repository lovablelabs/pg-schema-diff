package migration_acceptance_tests

import (
	"testing"

	"github.com/stripe/pg-schema-diff/pkg/diff"
)

// Test case demonstrating circular dependency bug when:
// 1. Two tables have circular policy dependencies (A -> B, B -> A)
// 2. Both tables have triggers using a function
// 3. The function is deleted (not in target schema)
// 4. One table is altered (e.g., adding a column)
var circularDependencyTestCases = []acceptanceTestCase{
	{
		name: "Circular policy dependency with function deletion causes cycle",
		oldSchemaDDL: []string{
			`
				-- Trigger function that will be deleted
				CREATE OR REPLACE FUNCTION update_updated_at_column()
				RETURNS TRIGGER AS $$
				BEGIN
					NEW.updated_at = now();
					RETURN NEW;
				END;
				$$ LANGUAGE plpgsql;

				-- Table A with trigger
				CREATE TABLE table_a (
					id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
					name TEXT,
					updated_at TIMESTAMPTZ DEFAULT now()
				);

				CREATE TRIGGER update_table_a_updated_at
					BEFORE UPDATE ON table_a
					FOR EACH ROW
					EXECUTE FUNCTION update_updated_at_column();

				ALTER TABLE table_a ENABLE ROW LEVEL SECURITY;

				-- Table B with trigger
				CREATE TABLE table_b (
					id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
					a_id UUID REFERENCES table_a(id),
					name TEXT,
					updated_at TIMESTAMPTZ DEFAULT now()
				);

				CREATE TRIGGER update_table_b_updated_at
					BEFORE UPDATE ON table_b
					FOR EACH ROW
					EXECUTE FUNCTION update_updated_at_column();

				ALTER TABLE table_b ENABLE ROW LEVEL SECURITY;

				-- Circular policy dependencies
				CREATE POLICY table_a_policy ON table_a
					FOR ALL
					USING (EXISTS (SELECT 1 FROM table_b WHERE a_id = table_a.id));

				CREATE POLICY table_b_policy ON table_b
					FOR ALL
					USING (EXISTS (SELECT 1 FROM table_a WHERE id = table_b.a_id));
			`,
		},
		newSchemaDDL: []string{
			`
				-- Function NOT defined (will be deleted)

				-- Table A altered (new column added)
				CREATE TABLE table_a (
					id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
					name TEXT,
					new_column TEXT,  -- NEW COLUMN
					updated_at TIMESTAMPTZ DEFAULT now()
				);

				-- No trigger (will be dropped)

				ALTER TABLE table_a ENABLE ROW LEVEL SECURITY;

				-- Table B unchanged
				CREATE TABLE table_b (
					id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
					a_id UUID REFERENCES table_a(id),
					name TEXT,
					updated_at TIMESTAMPTZ DEFAULT now()
				);

				-- No trigger (will be dropped)

				ALTER TABLE table_b ENABLE ROW LEVEL SECURITY;

				-- Same circular policies
				CREATE POLICY table_a_policy ON table_a
					FOR ALL
					USING (EXISTS (SELECT 1 FROM table_b WHERE a_id = table_a.id));

				CREATE POLICY table_b_policy ON table_b
					FOR ALL
					USING (EXISTS (SELECT 1 FROM table_a WHERE id = table_b.a_id));
			`,
		},
		// This should fail with a cycle detection error
		// Until the bug is fixed, mark as expected error
		expectedPlanErrorContains: "cycle detected",
	},
	{
		name: "Same scenario but function is kept - should work",
		planOpts: []diff.PlanOpt{
			// Skip validation because the target schema has circular policy dependencies
			// that cannot be created from scratch (validation limitation)
			diff.WithDoNotValidatePlan(),
		},
		oldSchemaDDL: []string{
			`
				-- Trigger function
				CREATE OR REPLACE FUNCTION update_updated_at_column()
				RETURNS TRIGGER AS $$
				BEGIN
					NEW.updated_at = now();
					RETURN NEW;
				END;
				$$ LANGUAGE plpgsql;

				-- Table A with trigger
				CREATE TABLE table_a (
					id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
					name TEXT,
					updated_at TIMESTAMPTZ DEFAULT now()
				);

				CREATE TRIGGER update_table_a_updated_at
					BEFORE UPDATE ON table_a
					FOR EACH ROW
					EXECUTE FUNCTION update_updated_at_column();

				ALTER TABLE table_a ENABLE ROW LEVEL SECURITY;

				-- Table B with trigger
				CREATE TABLE table_b (
					id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
					a_id UUID REFERENCES table_a(id),
					name TEXT,
					updated_at TIMESTAMPTZ DEFAULT now()
				);

				CREATE TRIGGER update_table_b_updated_at
					BEFORE UPDATE ON table_b
					FOR EACH ROW
					EXECUTE FUNCTION update_updated_at_column();

				ALTER TABLE table_b ENABLE ROW LEVEL SECURITY;

				-- Circular policy dependencies
				CREATE POLICY table_a_policy ON table_a
					FOR ALL
					USING (EXISTS (SELECT 1 FROM table_b WHERE a_id = table_a.id));

				CREATE POLICY table_b_policy ON table_b
					FOR ALL
					USING (EXISTS (SELECT 1 FROM table_a WHERE id = table_b.a_id));
			`,
		},
		newSchemaDDL: []string{
			`
				-- Function KEPT in schema (not deleted)
				CREATE OR REPLACE FUNCTION update_updated_at_column()
				RETURNS TRIGGER AS $$
				BEGIN
					NEW.updated_at = now();
					RETURN NEW;
				END;
				$$ LANGUAGE plpgsql;

				-- Table A altered (new column added)
				CREATE TABLE table_a (
					id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
					name TEXT,
					new_column TEXT,  -- NEW COLUMN
					updated_at TIMESTAMPTZ DEFAULT now()
				);

				-- Trigger kept
				CREATE TRIGGER update_table_a_updated_at
					BEFORE UPDATE ON table_a
					FOR EACH ROW
					EXECUTE FUNCTION update_updated_at_column();

				ALTER TABLE table_a ENABLE ROW LEVEL SECURITY;

				-- Table B unchanged
				CREATE TABLE table_b (
					id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
					a_id UUID REFERENCES table_a(id),
					name TEXT,
					updated_at TIMESTAMPTZ DEFAULT now()
				);

				-- Trigger kept
				CREATE TRIGGER update_table_b_updated_at
					BEFORE UPDATE ON table_b
					FOR EACH ROW
					EXECUTE FUNCTION update_updated_at_column();

				ALTER TABLE table_b ENABLE ROW LEVEL SECURITY;

				-- Same circular policies
				CREATE POLICY table_a_policy ON table_a
					FOR ALL
					USING (EXISTS (SELECT 1 FROM table_b WHERE a_id = table_a.id));

				CREATE POLICY table_b_policy ON table_b
					FOR ALL
					USING (EXISTS (SELECT 1 FROM table_a WHERE id = table_b.a_id));
			`,
		},
		// This should work because function isn't deleted
		// so circular tables aren't pulled into the graph.
		// The fix prevents table_b from being added to the graph since it has no changes,
		// which breaks the circular dependency.
		expectedDBSchemaDDL: []string{
			`
				-- Function KEPT in schema (not deleted)
				CREATE OR REPLACE FUNCTION update_updated_at_column()
				RETURNS TRIGGER AS $$
				BEGIN
					NEW.updated_at = now();
					RETURN NEW;
				END;
				$$ LANGUAGE plpgsql;

				-- Table A with new column added at the end
				CREATE TABLE table_a (
					id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
					name TEXT,
					updated_at TIMESTAMPTZ DEFAULT now(),
					new_column TEXT  -- Added at the end via ALTER TABLE ADD COLUMN
				);

				-- Trigger kept
				CREATE TRIGGER update_table_a_updated_at
					BEFORE UPDATE ON table_a
					FOR EACH ROW
					EXECUTE FUNCTION update_updated_at_column();

				ALTER TABLE table_a ENABLE ROW LEVEL SECURITY;

				-- Table B unchanged
				CREATE TABLE table_b (
					id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
					a_id UUID REFERENCES table_a(id),
					name TEXT,
					updated_at TIMESTAMPTZ DEFAULT now()
				);

				-- Trigger kept
				CREATE TRIGGER update_table_b_updated_at
					BEFORE UPDATE ON table_b
					FOR EACH ROW
					EXECUTE FUNCTION update_updated_at_column();

				ALTER TABLE table_b ENABLE ROW LEVEL SECURITY;

				-- Same circular policies
				CREATE POLICY table_a_policy ON table_a
					FOR ALL
					USING (EXISTS (SELECT 1 FROM table_b WHERE a_id = table_a.id));

				CREATE POLICY table_b_policy ON table_b
					FOR ALL
					USING (EXISTS (SELECT 1 FROM table_a WHERE id = table_b.a_id));
			`,
		},
	},
}

func TestCircularDependencyBug(t *testing.T) {
	runTestCases(t, circularDependencyTestCases)
}
