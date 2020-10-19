# Referring to: Kaggle competition - predict stock price movement based on News Headline using NLP

# Load data into Google Colab (if the tool is Colab, else skip):
from google.colab import drive
drive.mount("/content/gdrive")
import pandas as pd
df = pd.read_csv('/content/gdrive/My Drive/Data Science code vault/Colab Notebooks/NLP/Data.csv', encoding="ISO-8859-1")

# Cleaning texts: removing punctuations and special characters in the headlines
data = train.iloc[:, 2:27]
data.replace("[^a-zA-Z]", " ", regex=True, inplace= True)

# Rename columns for ease of access: 
new_index_list = [str(i) for i in range(25)]
data.columns = new_index_list
data.head(5)

# Converting headlines to lower case
for index in new_index_list:
  data[index] = data[index].str.lower()
data.head(1)

