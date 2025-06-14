import random

def create_test_file(filename, target_size_mb):
    """Create a test file of approximately specified size in MB using readable text."""
    # List of sample words and sentences to make somewhat readable content
    words = ["the", "be", "to", "of", "and", "a", "in", "that", "have", "I", 
            "it", "for", "not", "on", "with", "he", "as", "you", "do", "at"]
    sentences = [
        "This is a test file containing random text.",
        "The quick brown fox jumps over the lazy dog.",
        "Lorem ipsum dolor sit amet, consectetur adipiscing elit.",
        "Python is a versatile programming language.",
        "Data processing requires careful handling of files."
    ]
    
    with open(filename, 'w', encoding='utf-8') as f:
        target_bytes = target_size_mb * 1024 * 1024
        current_size = 0
        
        while current_size < target_bytes:
            # Randomly choose between writing a sentence or random words
            if random.random() < 5:
                text = random.choice(sentences) + " "
            else:
                text = " ".join(random.choices(words, k=random.randint(5, 15))) + ". "
            
            f.write(text)
            current_size = f.tell()

# Create test files of different sizes
sizes = {
    'test_1mb.txt': 1,
    'test_10mb.txt': 10,
    'test_50mb.txt': 50,
    'test_100mb.txt': 100,
    'test_250mb.txt': 250,
    'test_1gb.txt': 500
}

for filename, size in sizes.items():
    print(f"Creating {filename} ({size}MB)...")
    create_test_file(filename, size)
    print(f"Created {filename}")
