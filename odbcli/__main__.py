from multiprocessing import set_start_method
from .cli import main
#main()

if __name__ == "__main__":
    set_start_method('spawn')
    main()
