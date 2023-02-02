#%% 
import sourceToBronzeV2
import bronzeToSilver
import silverToGold
import time

def init():
    print("Executing entire ETL...")
    sourceToBronzeV2.init()
    bronzeToSilver.init()
    silverToGold.init()

if __name__ == "__main__":
    init()



# %%
