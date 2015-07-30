.PHONY: all
all:
	sh gobuild.sh

.PHONY: clean
clean:
	rm mst cli srv
