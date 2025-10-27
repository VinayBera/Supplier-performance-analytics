param(
    [Parameter(Mandatory=$true)]
    [string]$RepoUrl
)

git init
git add .
git commit -m "Initial commit: OpsPulse analytics"
git branch -M main
git remote add origin $RepoUrl
git push -u origin main
