# Poetry Generator

A Java-based poetry generation system that uses Markov chains and hash tables to create poems based on word frequency analysis. The trained version is trained off of the english poems at https://zenodo.org/records/10907309

## Overview

This Poetry Generator analyzes text files to build statistical models of word relationships and generates original poetry. It uses a custom hash table implementation with quadratic probing for efficient word lookup and frequency tracking.

## Quick Start

### Using the Pre-trained Model (Recommended)

The easiest way to get started is with the pre-trained model:

```bash
cd trained
java HashingPoetry
```

Enter a starting word and poem length when prompted.

### Training Your Own Model

To train the generator on your own text data:

1. Place your text files in the `untrained/data/` folder
   - Use plain text files (.txt format) or JSON files (.json format)
   - Each file should contain the text you want to train on (poems, stories, etc.)
   - The cleaning script will combine and preprocess all .txt and .json files in the folder
2. Run the cleaning script to preprocess the data:
   ```bash
   cd untrained/data
   python combine_and_clean_v2.py
   ```
3. Compile the Java files:
   ```bash
   cd ../
   javac *.java
   ```
4. Run the generator (this will train on your data):
   ```bash
   java HashingPoetry
   ```

## Project Structure

```
poetryGenerator/
├── trained/                    # Compiled Java classes
├── untrained/                 # Source code
│   ├── HashingPoetry.java     # Main application
│   ├── WritePoetry.java       # Poetry generation engine
│   ├── HashTable.java         # Custom hash table implementation
│   ├── WordFreqInfo.java      # Word frequency tracking
│   ├── ProgressBar.java       # Progress display utility
│   └── data/
│       └── combine_and_clean_v2.py  # Text preprocessing script
└── README.md
```