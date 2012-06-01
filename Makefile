SUBDIRS = udt common security gmp master slave client tools examples
TARGETS = clean all install

subdirs:
	@for dir in $(SUBDIRS); do \
		$(MAKE) -C $$dir || exit $$?; \
		$(MAKE) -C $$dir install; \
	done

clean:
	@for dir in $(SUBDIRS); do \
		$(MAKE) -C $$dir clean; \
	done
