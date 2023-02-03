#%% 
import bronze
import silver
import gold
import time

def init():
    print("Executing entire ETL...")
    bronze.init()
    silver.init()
    gold.init()

if __name__ == "__main__":
    init()



# %%
