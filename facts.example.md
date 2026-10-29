# Fact bank — single source of truth

Copy this file to facts.md and replace every example line with your own facts.
facts.md is git-ignored; it never leaves your machine unless you send it to a
model backend yourself.

Every claim the agent makes must be a verbatim span from this file. Code checks
the span exists; if it does not, the requirement is graded as a gap. So this
file has to say the same things your resumes say, in copyable sentences.

Rules:
- One fact per line, self-contained. A rambling line makes a weak quote.
- Write the honest version. "Conceptual only, not production" is useful — it
  makes the model grade a requirement as partial instead of met.
- Nothing aspirational. This is what you can defend in an interview.
- When you change a resume, change this file in the same sitting.

---

## Identity

Example: B.Tech in Computer Science, graduated 2025.
Example: Open to relocate. Immediate joiner.

## Languages

Example: Python is my primary language for backend services and data pipelines.
Example: Java 21 with Spring Boot 3 for one public service.

## Backend and APIs

Example: Built REST APIs in FastAPI with request validation and typed error contracts.

## Cloud

Example: Deployed event-driven services on AWS Lambda and GCP Cloud Run.

## Project: example-service

Example: Built an idempotent payment API; retries are safe because every request carries an idempotency key.
Example: Repository is github.com/your-handle/example-service.

## Standing gaps — keep these so the model grades honestly

Example: No production React or mobile work.
Example: No Kafka or gRPC experience.
