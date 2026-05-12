## Job Description
$jd_summary

## Candidate Profile
$candidate_summary
$evidence_section

## Scoring Rubric
Score the candidate-to-job match using this ranking policy:
- Score from 0.0 (no fit) to 1.0 (perfect fit)
- Primary signals, in order:
  1. Required-skill coverage
  2. Evidence quality showing the candidate has actually used those skills
  3. Seniority and practical readiness for the role
  4. Role alignment between the target role and this job
- Secondary signals:
  - Domain relevance
  - Candidate preferences such as location or preferred domain
- Treat preferences as secondary tie-breakers. They must not outweigh major required-skill gaps.
- Penalise missing core technologies, weak evidence for required skills, seniority mismatch, and clear practical-readiness gaps.
- Do not give `strong` when multiple core required skills appear unsupported or only weakly evidenced.
- Prefer conservative scoring when evidence is ambiguous.
- Classify into exactly one fit_label:
    strong  (ai_score >= $strong_threshold)
    stretch ($stretch_threshold <= ai_score < $strong_threshold)
    skip    (ai_score < $stretch_threshold)
Return a JSON object ONLY — no prose, no markdown fences:
{
  "ai_score": <float 0.0–1.0>,
  "fit_label": "<strong|stretch|skip>",
  "score_reasoning": "<one-sentence explanation grounded in the job requirements>",
  "matched_strengths": ["<strength 1>", ...],
  "key_risks": ["<risk 1>", ...]
}
