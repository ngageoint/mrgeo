package org.mrgeo.cmd.findholes;

import org.mrgeo.cmd.Command;
import org.mrgeo.cmd.CommandSpi;

public class FindHolesSpi extends CommandSpi{

	@Override
	public Class<? extends Command> getCommandClass() {
		return FindHoles.class;
	}

	@Override
	public String getCommandName() {
		return "findholes";
	}

	@Override
	public String getDescription() {
		return "Find holes for an image (developer cmd)";
	}

}
