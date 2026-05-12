CREATE TABLE IF NOT EXISTS `fitcv-491123.fitcv.final_ranking` (
  job_url           STRING    NOT NULL OPTIONS(description="LinkedIn job posting URL (primary key)"),
  final_rank        INT64     NOT NULL OPTIONS(description="Final composite rank (1 = best fit)"),
  final_score       FLOAT64   NOT NULL OPTIONS(description="Weighted composite score [0.0, 1.0]"),
  ai_score          FLOAT64   NOT NULL OPTIONS(description="AI score from ai_score_results"),
  must_have_match   FLOAT64   NOT NULL OPTIONS(description="Ratio of matched required skills"),
  vector_similarity FLOAT64   NOT NULL OPTIONS(description="Cosine similarity from vector_shortlist"),
  title_relevance   FLOAT64   NOT NULL OPTIONS(description="Title token overlap score"),
  seniority_fit     FLOAT64   NOT NULL OPTIONS(description="Seniority mapped score (exact=1.0, ±1=0.5, ±2=0.0)"),
  preference_fit    FLOAT64   NOT NULL OPTIONS(description="Domain and location preference match ratio"),
  fit_label         STRING    NOT NULL OPTIONS(description="Label inherited from ai_score_results (strong, stretch, skip)"),
  ranked_at         TIMESTAMP NOT NULL
);
