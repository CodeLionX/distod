package de.metanome.algorithms.hyfd;

import de.metanome.algorithms.hyfd.config.Config;
import de.metanome.algorithms.hyfd.mocks.MetanomeMock;

public class DistodTestRunner {

	public void run() {
		Config conf = new Config();
		MetanomeMock.executeDistod(conf);
	}

	private void wrongArguments(String[] args) {
		StringBuilder message = new StringBuilder();
		message.append("\r\nArguments not supported: ").append(args);
		// TODO: message.append("\r\nProvide correct values: <algorithm> <database> <dataset> <inputTableLimit>");
		throw new RuntimeException(message.toString());
	}
	
}
