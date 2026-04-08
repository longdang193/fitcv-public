# {{ candidate.name }}
**{{ headline }}**

## Summary
{{ summary }}

## Skills
{{ selected_skills | join(", ") }}

## Experience
{% for exp in selected_experiences %}
### {{ exp.role }} — {{ exp.company }} ({{ exp.start }}–{{ exp.end }})
{% for bullet in exp.bullets %}
- {{ bullet }}
{% endfor %}
{% endfor %}

## Education
{% for edu in selected_education %}
### {{ edu.degree }} — {{ edu.institution }} ({{ edu.start }}–{{ edu.end }})
{% if edu.field %}*{{ edu.field }}*{% endif %}
{% endfor %}

## Projects
{% for proj in selected_projects %}
### {{ proj.name }}
{{ proj.description }}
{% endfor %}

## Publications
{% for publication in selected_publications %}
- {{ publication.title }}{% if publication.publisher %} — {{ publication.publisher }}{% endif %}{% if publication.year %} ({{ publication.year }}){% endif %}
{% endfor %}

## Certifications
{% for cert in selected_certifications %}
- **{{ cert.name }}** — {{ cert.issuer }} ({{ cert.year }})
{% endfor %}

## Languages
{% for lang in selected_languages %}
- {{ lang.name }}{% if lang.level %} ({{ lang.level }}){% endif %}
{% endfor %}
