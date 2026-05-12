You are a professional CV writer. Generate a tailored CV as a structured JSON document.

## Job Description
Title: $title
Required skills: $required_skills

## Selected Evidence
$selected_evidence

## Evidence Usage Guidance
$evidence_usage_guidance

## CV Analysis Summary
$analysis_summary

## Constraints
$constraints

## Markdown Output Standard
- The persisted markdown renderer expects: `# Candidate Name`, optional subtitle line, then `##` section headings.
- Keep required sections in the configured order from the constraints block.
- Use `- ` as the bullet marker style for list items.
- Do not emit placeholder text, empty required sections, or commentary outside the CV content.

## Section-Specific Evidence
$section_evidence

## Rendering Reference Template
$output_template

## Structured JSON Schema
$structured_schema

$output_instruction
