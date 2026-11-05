# BrightCart Daily Sales Pipeline

Mini project, Week 7 and Week 8.

BrightCart, a small UK online retailer, receives two files every morning: `customers.csv` and `orders.csv`. We will turn them into a daily sales report we can trust.

## Today: Week 7, Day 2

Two things:

1. **Create the project folder**, inside the course folder from last week's Airflow setup.
2. **Do the lab** (Q1 to Q16, Q17 to Q20 optional). The data is created inside the code, so you do not need any file yet.

## Setup (Mac and Windows)

We reuse the course folder (`data_course`) and the Airflow environment (`.venv_airflow`) from last week's setup guide. Nothing new to install except the lab libraries.

The commands are **the same on Mac and Windows**. Run them in the VS Code terminal (Terminal, New Terminal), **one line at a time**.

> **Windows:** first check that the bottom left corner of VS Code shows **WSL: Ubuntu 24.04**. If it does not, click the blue `><` button and choose **Connect to WSL**. All the commands below run in the Ubuntu terminal (the line ends with `$`).

### 1. Go to the course folder and activate the environment

```bash
cd ~/data_course
source .venv_airflow/bin/activate
```

The terminal line must now start with `(.venv_airflow)`.

### 2. Install the libraries for the lab

```bash
pip install pandas numpy ipykernel
```

### 3. Create the project folder and its structure

```bash
mkdir brightcart_daily_sales_pipeline
cd brightcart_daily_sales_pipeline
mkdir data notebooks scripts dags data/raw data/cleaned data/curated data/audit
```

**Prefer clicking?** In the VS Code Explorer, use the **New Folder** icon and create the same folders by hand.

### 4. Open the project in VS Code

```bash
code .
```

### 5. Create your lab notebook

In the new VS Code window: right click `notebooks`, **New File**, name it `01_lab_build.ipynb`.

Top right of the notebook: **Select Kernel**, **Python Environments**, choose **.venv_airflow**.

On Windows, the new window must also show **WSL: Ubuntu 24.04** in the bottom left corner.

### 6. Test

In the first cell:

```python
import pandas as pd
print(pd.__version__)
```

If a version number appears, you are ready for the lab.

> **Error or `command not found`?** Stop and send a screenshot. If `pip` or `python` is not found, the environment is not active: repeat step 1.
>
> **Using Plan B (GitHub Codespaces)?** Send me a message: your course folder is in a different place.

## Folder structure

Your course folder now looks like this:

```
data_course/
    .venv_airflow/                       the environment (don't touch)
    airflow/                             Airflow files (from last week)
    labs/                                theory answers (from last week)
    brightcart_daily_sales_pipeline/     NEW: the mini project
        data/
            raw/
            cleaned/
            curated/
            audit/
        notebooks/
            01_lab_build.ipynb
        scripts/
        dags/
        README.md
```

| Folder | What goes there |
|---|---|
| `data/raw/` | the daily files, exactly as received (next class) |
| `data/cleaned/` | valid rows, and rejected rows with a reason |
| `data/curated/` | the report: revenue by city, daily summary |
| `data/audit/` | the evidence: counts, revenue, quality checks |
| `notebooks/` | your lab today, and later the pipeline step by step |
| `scripts/` | the pipeline functions (next class) |
| `dags/` | the Airflow DAG (Week 8) |

Most folders stay **empty for now**. They fill up in the next classes.

> Git does not save empty folders. If you push to GitHub today, create an empty file called `.gitkeep` inside each empty folder so the structure is kept.
