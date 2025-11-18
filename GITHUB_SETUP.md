# GitHub Page Setup

## 🌐 Create Your GitHub Repository & Page

### Step 1: Run Setup Script

```bash
./setup-github.sh YOUR_GITHUB_USERNAME
```

**Example:**
```bash
./setup-github.sh asimkumarrout
```

This will:
- Initialize git
- Commit all files
- Create GitHub repository (if `gh` CLI is installed)
- Push code to GitHub

### Step 2: Enable GitHub Pages

1. Go to your repository settings:
   ```
   https://github.com/YOUR_USERNAME/firebolt-cdc-lambda/settings/pages
   ```

2. Under "Source":
   - Select: **Deploy from a branch**
   - Branch: **main**
   - Folder: **/ (root)**

3. Click **Save**

4. Wait 1-2 minutes

5. Visit your page:
   ```
   https://YOUR_USERNAME.github.io/firebolt-cdc-lambda/
   ```

## 📦 If GitHub CLI Not Installed

### Option A: Install GitHub CLI

**macOS:**
```bash
brew install gh
gh auth login
```

**Linux:**
```bash
sudo apt install gh
gh auth login
```

**Windows:**
```bash
winget install --id GitHub.cli
gh auth login
```

### Option B: Manual Setup

1. Create repo on GitHub:
   - Go to https://github.com/new
   - Repository name: `firebolt-cdc-lambda`
   - Public
   - Don't initialize with README
   - Create repository

2. Push code:
   ```bash
   git remote add origin https://github.com/YOUR_USERNAME/firebolt-cdc-lambda.git
   git branch -M main
   git push -u origin main
   ```

3. Enable GitHub Pages (see Step 2 above)

## ✅ Verify

Your documentation will be live at:
```
https://YOUR_USERNAME.github.io/firebolt-cdc-lambda/
```

Share this URL with your client! 🎉



