# Atriuum Import Rules Troubleshooting

## Common Error Causes

### 1. Field Name Issues
Atriuum may not recognize these field names:
- `Bibliographic.PublicationPlace` → Try `Bibliographic.PubPlace` 
- `Bibliographic.CopyrightDate` → Try `Bibliographic.CopyrightDate` (check spelling)
- Complex operator methods may not be supported

### 2. Operator Method Compatibility
Avoid these if causing errors:
- `regexreplace` - May not be supported
- Complex `compositeoperator` with multiple rules
- Advanced string manipulation

### 3. Precedence Conflicts
Ensure precedence values don't conflict with existing rules

## Step-by-Step Testing Approach

### Phase 1: Test Individual Rules
1. **Start with just one new rule** (easiest: Publisher from 264a)
2. Import and test
3. If successful, add the next rule
4. If fails, try the alternative field names below

### Phase 2: Alternative Field Names to Try

**For Publisher:**
```xml
<Rule RuleName="Publisher" FieldName="Bibliographic.PublisherName" ...>
<Rule RuleName="Publisher" FieldName="Bibliographic.Publisher" ...>
```

**For Publication Place:**
```xml
<Rule RuleName="PubPlace" FieldName="Bibliographic.PubPlace" ...>
<Rule RuleName="PubPlace" FieldName="Bibliographic.PublicationPlace" ...>
```

**For Cost:**
```xml
<Rule RuleName="Cost" FieldName="Holdings.ItemPrice" ...>
<Rule RuleName="Cost" FieldName="Holdings.Price" ...>
```

### Phase 3: Minimal Working Example
```xml
<RuleSet RuleSetName="Test Rules" ImportOrExport="import" version="2">
	<!-- TEST 1: Publisher only -->
	<Rule RuleName="Test Publisher" FieldName="Bibliographic.Publisher" Precedence="100" BooleanOperator="or" OperatorMethod="trim">
		<Variable Name="ltrim" Value="0"/>
		<Variable Name="marcfield" Value="264a"/>
		<Variable Name="rtrim" Value="0"/>
	</Rule>
	
	<!-- TEST 2: Cost only -->
	<Rule RuleName="Test Cost" FieldName="Holdings.Cost" Precedence="101" BooleanOperator="or" OperatorMethod="trim">
		<Variable Name="ltrim" Value="0"/>
		<Variable Name="marcfield" Value="020c"/>
		<Variable Name="rtrim" Value="0"/>
	</Rule>
</RuleSet>
```

## Error Diagnosis

### If you get an error message:
1. **Note the exact error text** - it often indicates which rule/field is problematic
2. **Check field names** against your Atriuum field configuration
3. **Simplify complex rules** - break them into smaller pieces
4. **Test operators individually** - some may not be supported

### Common Solutions:
1. **Use basic operators**: `trim`, `notequals`, `substring`
2. **Avoid regex**: Use `substring` for date cleaning instead
3. **Check field existence**: Ensure the target Atriuum fields exist
4. **Lower precedence**: Use higher numbers (100+) for new rules to avoid conflicts

## Fallback Position

If custom rules continue to fail, we can:
1. **Pre-process MARC records** with Python to clean data before import
2. **Use Atriuum's built-in field mappings** where possible
3. **Manual mapping** through the Atriuum UI for critical fields

The key is to start simple and add complexity gradually, testing each step.