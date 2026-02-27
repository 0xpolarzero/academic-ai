## Skills
1. how to read/use/search docx (including using subagents for researching/searching and not bloating main agent context—can we have subagents in subagents?)
how to correctly write comments and suggestions in a docx (only select relevant part and not big paragraphs for easy apply)
2. how to split docx into atomic chunks; should make sense on their own and not be cut from each other, otherwise chunk n+1 might not make sense due to being separate from chunk n; each chunk should be small enough to not take too many tokens and be processed by one agent while taking only a small part of its context
  - QUESTION: what is a reasonable token limit? how much can we deviate to include relevant trailing content for the chunk?
  - include metadata on where it was taken from
3. how to aggregate results from multiple subagents and generate a final doc with comments/suggestions + a simple "before -> after" set of diffs with which page
  - There can be multiple suggestions that are the same that should be deduplicated in another subagent

Always place artifacts inside a `artifacts/` folder

## Orchestrator rompt
- Read the docx and separate it into atomic chunks using skill 1
- separate each chunk and place them into a `artifacts/chunks/` directory using skill 2
- Start a subagent for each chunk with each chunk prompt
- Once they are done aggregate the results using skill 3

## Chunk prompt
- You are ...
- Where to find your chunk: `artifacts/chunks/chunk_XXX`
- Strictly what you are tasked to do...
- Output format (suggestions? or is just diff better?)
