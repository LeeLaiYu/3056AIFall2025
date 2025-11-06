# Lab 3: Git Operations with AI Agents

## Overview
This lab teaches you how to use natural language instructions with AI agents to perform Git operations (commit, push) and create pull requests on GitHub. You'll learn how AI translates your instructions into CLI commands automatically.

## Objectives
- Learn to use natural language to instruct AI agents for Git operations
- Understand how AI generates CLI commands from your instructions
- Practice committing and pushing changes using AI assistance
- Create pull requests on GitHub through natural language commands

## Prerequisites
- Completion of Lab 0, Lab 1, and Lab 2
- Git installed on your system
- GitHub account
- Fork of the repository (https://github.com/tesolchina/3056AIFall2025) under your GitHub account
- Access to an AI coding assistant (like Cursor)
- GitHub CLI (gh) installed (for pull request creation)

## Instructions
See [INSTRUCTIONS.md](./INSTRUCTIONS.md) for detailed step-by-step instructions.

## Folder Structure
```
lab3/
├── README.md              # This file
└── INSTRUCTIONS.md        # Detailed instructions
```

## Workflow

1. **Fork the Repository** - Fork https://github.com/tesolchina/3056AIFall2025 to your GitHub account
2. **Clone Your Fork** - Clone your forked repository to your local machine
3. **Make Changes** - Create or modify files in your project
4. **Instruct AI** - Use natural language to ask AI to commit and push changes to your fork
5. **Verify Commands** - Review the CLI commands that AI generates
6. **Create Pull Request** - Use natural language to ask AI to create a PR from your fork to the main repository
7. **Document** - Document the executed commands in a markdown file

## Key Deliverables

1. **Changes Made** (in your project)
   - Files modified or created

2. **Documentation**
   - Document the CLI commands that were generated and executed (save as a markdown file)

## Understanding CLI and AI Command Generation

**What is CLI?**
- CLI (Command Line Interface) is a text-based way to interact with your computer
- You type commands that tell the computer what to do
- Git uses CLI commands like `git add`, `git commit`, `git push`

**How AI Generates CLI Commands:**
- You describe what you want in natural language (e.g., "commit my changes with message 'add new feature'")
- AI understands your intent and translates it into appropriate CLI commands
- AI generates commands like `git add .`, `git commit -m "add new feature"`, `git push`
- AI can execute these commands for you automatically

**Benefits:**
- No need to memorize exact Git syntax
- Natural language is more intuitive
- AI can suggest best practices
- Reduces errors from typos or incorrect syntax

## Resources
- Git documentation: https://git-scm.com/doc
- GitHub CLI documentation: https://cli.github.com/
- Main repository: https://github.com/tesolchina/3056AIFall2025
- How to fork a repository: https://docs.github.com/en/get-started/quickstart/fork-a-repo

## Notes
- Always review the commands AI generates before execution
- Make sure your commit messages are descriptive
- Ensure you have permission to push to the repository
- Pull requests should have clear titles and descriptions
- Test your changes before committing

## Example Natural Language Instructions

- "Commit all my changes with the message 'Update lab instructions'"
- "Push my commits to my fork on GitHub"
- "Create a pull request from my fork to the tesolchina/3056AIFall2025 repository with title 'Add new features' and description 'This PR adds new lab materials'"
- "Stage only the .md files and commit them"

