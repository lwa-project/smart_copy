#!/bin/bash

SCRIPT='smart-copy'

DEFAULT_PATH=`grep EXEC_PATH= $SCRIPT | sed -e 's/.*=//g' `
DEFAULT_USER=`grep EXEC_USER= $SCRIPT | sed -e 's/.*=//g' `
USES_SCREEN=`grep screen $SCRIPT`

echo "This will install $SCRIPT assuming:"
echo " 1) The user $DEFAULT_USER will be running it and"
echo " 2) The executable is located in $DEFAULT_PATH"
if [[ "$USES_SCREEN" != "" ]]; then
	echo " 3) The service will be run in a screen session"
fi
echo ""
read -p "Is this ok? [y/N] " yesno
if [[ "$yesno" == "y" || "$yesno" == "Y" ]]; then
	if [[ "$USES_SCREEN" != "" && `which screen` == "" ]]; then
		echo "screen needed but not in the current \$PATH, aborting"
		exit 1
	fi
	
	cp $SCRIPT /etc/init.d/
	update-rc.d $SCRIPT defaults 40
else
	echo "Aborting"
	exit 1
fi

