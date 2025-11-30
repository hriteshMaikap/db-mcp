import matplotlib.pyplot as plt
import io
import base64
import os
from typing import List, Any, Optional

def save_chart(fig, filename: str) -> str:
    """Saves the figure to a file and returns the relative path."""
    # Ensure reports directory exists
    os.makedirs("reports", exist_ok=True)
    filepath = os.path.join("reports", filename)
    fig.savefig(filepath)
    plt.close(fig)
    return filepath

def create_bar_chart(x_values: List[Any], y_values: List[float], title: str, xlabel: str, ylabel: str, filename: str) -> str:
    """Generates a bar chart and saves it to disk."""
    fig = plt.figure(figsize=(10, 6))
    plt.bar([str(x) for x in x_values], y_values, color='skyblue')
    plt.title(title)
    plt.xlabel(xlabel)
    plt.ylabel(ylabel)
    plt.xticks(rotation=45, ha='right')
    plt.tight_layout()
    return save_chart(fig, filename)

def create_pie_chart(labels: List[str], sizes: List[float], title: str, filename: str) -> str:
    """Generates a pie chart and saves it to disk."""
    fig = plt.figure(figsize=(8, 8))
    plt.pie(sizes, labels=labels, autopct='%1.1f%%', startangle=140)
    plt.title(title)
    plt.axis('equal')
    return save_chart(fig, filename)

def create_line_chart(x_values: List[Any], y_values: List[float], title: str, xlabel: str, ylabel: str, filename: str) -> str:
    """Generates a line chart and saves it to disk."""
    fig = plt.figure(figsize=(10, 6))
    plt.plot([str(x) for x in x_values], y_values, marker='o', linestyle='-', color='green')
    plt.title(title)
    plt.xlabel(xlabel)
    plt.ylabel(ylabel)
    plt.grid(True)
    plt.xticks(rotation=45, ha='right')
    plt.tight_layout()
    return save_chart(fig, filename)

def create_scatter_chart(x_values: List[float], y_values: List[float], title: str, xlabel: str, ylabel: str, filename: str) -> str:
    """Generates a scatter chart and saves it to disk."""
    fig = plt.figure(figsize=(10, 6))
    plt.scatter(x_values, y_values, color='purple', alpha=0.6)
    plt.title(title)
    plt.xlabel(xlabel)
    plt.ylabel(ylabel)
    plt.grid(True)
    plt.tight_layout()
    return save_chart(fig, filename)

